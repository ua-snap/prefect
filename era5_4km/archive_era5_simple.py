from pathlib import Path
import paramiko
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from datetime import datetime
import os

# SSH connection details for Chinook HPC
SSH_HOST = "chinook04.rcs.alaska.edu"
SSH_PORT = 22

"""
Simple ERA5 Data Archival Flow

This flow provides simple, reliable archival of completed ERA5 data to Poseidon storage.

DESIGN PHILOSOPHY:
==================

- **Simplicity First**: Uses straightforward tar + scp commands
- **Manual Control**: User decides when processing is complete  
- **Zero Coupling**: Completely independent from main processing flow
- **Selective Archiving**: Archive specific variables or all available
- **Fail-Fast**: Clear error handling without complex retry logic

USAGE:
======

# Archive specific variables after processing completes
python archive_era5_simple.py --variables "t2_mean,t2_min,t2_max"

# Archive all available variables  
python archive_era5_simple.py --variables "all"

# Archive with custom paths
archive_era5_simple(
    source_directory="/beegfs/CMIP6/snapdata/curated_wrf_era5-04km",
    destination_directory="/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km",
    variables="t2_mean"
)

PROCESS:
========

1. Discover available variable directories in source location
2. Create simple tar.gz archives on Chinook using basic tar command
3. Transfer directly to Poseidon via scp
4. Verify files exist on destination
5. Optional cleanup of source archives
6. Generate simple completion report

BENEFITS:
=========

- No race conditions (manual timing control)
- Simple to understand and debug
- Independent execution
- Fast and reliable for completed data
"""


def execute_ssh_with_logging(ssh: paramiko.SSHClient, command: str, description: str, remote_name: str = "chinook"):
    """Execute a single SSH command with logging, with agent forwarding."""
    logger = get_run_logger()
    logger.info(f"[{remote_name}] {description}")
    logger.debug(f"{remote_name} CMD: {command}")

    # Open a new channel for the session from the existing transport
    channel = ssh.get_transport().open_session()
    
    # Set up the agent request handler to handle agent requests from the server
    paramiko.agent.AgentRequestHandler(channel)
    
    # Execute the command on the channel
    channel.exec_command(command)
    
    # Read output, error, and exit status from the channel
    exit_status = channel.recv_exit_status()
    out = channel.makefile("r", -1).read().decode("utf-8").strip()
    err = channel.makefile_stderr("r", -1).read().decode("utf-8").strip()

    if exit_status != 0:
        msg = f"SSH command failed – {description} (exit {exit_status})\nCMD: {command}"
        if err:
            msg += f"\nSTDERR: {err}"
        if out:
            msg += f"\nSTDOUT: {out}"
        raise Exception(msg)

    if out:
        logger.debug(f"{remote_name} output: {out}")

    return out, err


@task
def discover_available_variables(ssh, source_directory: Path) -> list:
    """
    Find all variable directories available for archiving
    
    Args:
        ssh: Paramiko SSHClient connected to Chinook
        source_directory: Path to ERA5 output directory
        
    Returns:
        list: Variable names that have directories with data
    """
    logger = get_run_logger()
    source_dir = str(source_directory)
    
    logger.info(f"Discovering available variables in {source_dir}")
    
    # List subdirectories
    cmd = f"find '{source_dir}' -maxdepth 1 -type d -exec basename {{}} \\; | grep -v '^curated_wrf_era5-04km$' | sort"
    stdout, _ = execute_ssh_with_logging(ssh, cmd, "List variable directories")
    
    if not stdout:
        return []
    
    potential_vars = [line.strip() for line in stdout.split('\n') if line.strip()]
    
    # Verify each directory has data files
    verified_vars = []
    for var in potential_vars:
        var_path = f"{source_dir}/{var}"
        check_cmd = f"find '{var_path}' -name '*.nc' | head -1"
        
        try:
            file_check, _ = execute_ssh_with_logging(ssh, check_cmd, f"Check for data in {var}")
            if file_check:  # Found at least one .nc file
                verified_vars.append(var)
                logger.info(f"✅ {var}: Contains data files")
            else:
                logger.warning(f"⚠️ {var}: Directory exists but no .nc files found")
        except Exception as e:
            logger.warning(f"⚠️ {var}: Could not verify data files - {e}")
    
    logger.info(f"Found {len(verified_vars)} variables with data: {verified_vars}")
    return verified_vars


@task
def estimate_archive_size(ssh, source_directory: Path, variable_name: str) -> dict:
    """
    Estimate the size of archive that will be created
    
    Args:
        ssh: Paramiko SSHClient connected to Chinook
        source_directory: Path to ERA5 output directory
        variable_name: Variable to estimate
        
    Returns:
        dict: Size information
    """
    logger = get_run_logger()
    var_path = f"{source_directory}/{variable_name}"
    
    try:
        # Get directory size
        cmd = f"du -sh '{var_path}' | cut -f1"
        size_str, _ = execute_ssh_with_logging(ssh, cmd, f"Get size for {variable_name}")
        
        # Count files
        cmd = f"find '{var_path}' -name '*.nc' | wc -l"
        file_count, _ = execute_ssh_with_logging(ssh, cmd, f"Count files for {variable_name}")
        
        return {
            "variable": variable_name,
            "size": size_str.strip(),
            "file_count": int(file_count.strip())
        }
    except Exception as e:
        logger.warning(f"Could not estimate size for {variable_name}: {e}")
        return {
            "variable": variable_name,
            "size": "unknown",
            "file_count": 0
        }


@task
def archive_single_variable_simple(
    ssh, 
    source_directory: Path, 
    variable_name: str, 
    destination_directory: Path,
    ssh_username: str,
    cleanup_source_archive: bool = True
) -> dict:
    """
    Archive one variable using the simplest possible approach
    
    Args:
        ssh: Paramiko SSHClient connected to Chinook
        source_directory: Path to ERA5 output directory on Chinook
        variable_name: Variable to archive (e.g., 't2_mean')
        destination_directory: Path to backup storage on Poseidon
        ssh_username: Username for SSH connections
        cleanup_source_archive: Whether to remove tar file from Chinook after transfer
        
    Returns:
        dict: Archive result information
    """
    logger = get_run_logger()
    
    source_dir = str(source_directory)
    dest_dir = str(destination_directory)
    
    var_path = f"{source_dir}/{variable_name}"
    tar_filename = f"{variable_name}_era5_4km_archive.tar.gz"
    tar_path = f"{source_dir}/{tar_filename}"
    dest_var_dir = f"{dest_dir}/{variable_name}"
    dest_tar_path = f"{dest_var_dir}/{tar_filename}"
    
    logger.info(f"🗜️ Starting simple archive for variable: {variable_name}")
    
    start_time = datetime.now()
    
    try:
        # Step 1: Validate source directory exists and has content
        logger.info("📋 Validating source directory...")
        execute_ssh_with_logging(ssh, f"test -d '{var_path}'", f"Verify {variable_name} directory exists")
        execute_ssh_with_logging(ssh, f"test \"$(ls -A '{var_path}' 2>/dev/null)\"", f"Verify {variable_name} has content")
        
        # Step 2: Remove any existing tar file to avoid conflicts
        logger.info("🧹 Cleaning up any existing archive...")
        execute_ssh_with_logging(ssh, f"rm -f '{tar_path}'", "Remove existing tar file")
        
        # Step 3: Create tar archive using simple command
        logger.info(f"📦 Creating archive: {tar_filename}")
        cmd = f"cd '{source_dir}' && tar -czf '{tar_filename}' '{variable_name}/'"
        execute_ssh_with_logging(ssh, cmd, f"Create tar archive for {variable_name}")
        
        # Step 4: Verify archive was created and get size
        execute_ssh_with_logging(ssh, f"test -f '{tar_path}'", "Verify tar file was created")
        size_cmd = f"ls -lh '{tar_path}' | awk '{{print $5}}'"
        archive_size, _ = execute_ssh_with_logging(ssh, size_cmd, "Get archive size")
        logger.info(f"📦 Archive created: {archive_size.strip()}")
        
        # Step 5: Create destination directory on Poseidon
        logger.info("📁 Creating destination directory on Poseidon...")
        cmd = f"ssh {ssh_username}@poseidon.snap.uaf.edu 'mkdir -p \"{dest_var_dir}\"'"
        execute_ssh_with_logging(ssh, cmd, "Create destination directory on Poseidon")
        
        # Step 6: Transfer archive to Poseidon
        logger.info("📤 Transferring archive to Poseidon...")
        cmd = f"scp '{tar_path}' {ssh_username}@poseidon.snap.uaf.edu:'{dest_var_dir}/'"
        execute_ssh_with_logging(ssh, cmd, "Transfer archive to Poseidon")
        
        # Step 7: Verify transfer completed
        logger.info("✅ Verifying transfer...")
        cmd = f"ssh {ssh_username}@poseidon.snap.uaf.edu 'test -f \"{dest_tar_path}\" && ls -lh \"{dest_tar_path}\" | awk \"{{print \\$5}}\"'"
        remote_size, _ = execute_ssh_with_logging(ssh, cmd, "Verify file exists on Poseidon")
        
        if not remote_size:
            raise Exception("Archive file not found on Poseidon after transfer")
        
        logger.info(f"✅ Verified on Poseidon: {remote_size.strip()}")
        
        # Step 8: Optional cleanup
        if cleanup_source_archive:
            logger.info("🧹 Cleaning up source archive...")
            execute_ssh_with_logging(ssh, f"rm -f '{tar_path}'", "Remove source tar file")
            logger.info("✅ Source archive cleaned up")
        else:
            logger.info("📦 Source archive preserved on Chinook")
        
        end_time = datetime.now()
        duration = str(end_time - start_time)
        
        result = {
            "variable": variable_name,
            "status": "success",
            "archive_size": archive_size.strip(),
            "destination_path": dest_tar_path,
            "duration": duration,
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "cleanup_performed": cleanup_source_archive
        }
        
        logger.info(f"✅ Successfully archived {variable_name} in {duration}")
        return result
        
    except Exception as e:
        end_time = datetime.now()
        duration = str(end_time - start_time)
        
        logger.error(f"❌ Failed to archive {variable_name}: {str(e)}")
        
        # Attempt cleanup on failure
        try:
            execute_ssh_with_logging(ssh, f"rm -f '{tar_path}'", "Cleanup failed archive")
        except:
            pass  # Ignore cleanup failures
        
        return {
            "variable": variable_name,
            "status": "failed",
            "error": str(e),
            "duration": duration,
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S')
        }


@task
def create_simple_archive_report(results: dict, size_estimates: list) -> str:
    """
    Create a simple archival summary report
    
    Args:
        results: Dictionary with 'success' and 'failed' lists
        size_estimates: List of size estimation dictionaries
        
    Returns:
        str: Artifact ID
    """
    logger = get_run_logger()
    
    success_count = len(results["success"])
    failed_count = len(results["failed"])
    total_count = success_count + failed_count
    
    # Determine status
    if failed_count == 0:
        status_icon = "✅"
        status_text = "ALL SUCCESSFUL"
    elif success_count == 0:
        status_icon = "❌"
        status_text = "ALL FAILED"
    else:
        status_icon = "⚠️"
        status_text = "PARTIAL SUCCESS"
    
    # Build success section
    success_section = ""
    if results["success"]:
        for item in results["success"]:
            success_section += f"- **{item['variable']}**: {item['archive_size']} → `{item['destination_path']}` (took {item['duration']})\n"
    else:
        success_section = "*No variables were successfully archived*\n"
    
    # Build failure section
    failure_section = ""
    if results["failed"]:
        for item in results["failed"]:
            failure_section += f"- **{item['variable']}**: {item['error']} (failed after {item['duration']})\n"
    else:
        failure_section = "*No archival failures*\n"
    
    # Build size estimates section
    estimates_section = ""
    if size_estimates:
        for est in size_estimates:
            estimates_section += f"- **{est['variable']}**: {est['size']} ({est['file_count']} files)\n"
    
    # Calculate total duration
    if results["success"] or results["failed"]:
        all_items = results["success"] + results["failed"]
        start_times = [datetime.strptime(item['start_time'], '%Y-%m-%d %H:%M:%S') for item in all_items]
        end_times = [datetime.strptime(item['end_time'], '%Y-%m-%d %H:%M:%S') for item in all_items]
        
        total_start = min(start_times)
        total_end = max(end_times)
        total_duration = str(total_end - total_start)
    else:
        total_duration = "N/A"
    
    # Generate report
    markdown_content = f"""# 📦 Simple ERA5 Archival Report

**Status**: {status_icon} {status_text}  
**Completed**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Total Duration**: {total_duration}

## 📊 Summary

| Metric | Value |
|--------|-------|
| **Variables Processed** | {total_count} |
| **Successfully Archived** | {success_count} |
| **Failed Archives** | {failed_count} |
| **Success Rate** | {(success_count/total_count*100):.1f}% if {total_count} > 0 else 0% |

## 📏 Size Estimates

{estimates_section}

## ✅ Successfully Archived

{success_section}

## ❌ Failed Archives

{failure_section}

## 📋 Next Steps

"""
    
    if failed_count > 0:
        markdown_content += """
### 🔧 For Failed Archives:
1. Check error messages above for specific issues
2. Verify network connectivity to Poseidon
3. Ensure sufficient disk space on both systems
4. Re-run archival for failed variables: `--variables "failed_var_name"`

"""
    
    if success_count > 0:
        markdown_content += """
### ✅ Successful Archives:
1. Verify archive integrity at destination if needed
2. Archives are ready for use on Poseidon
3. Source data remains available on Chinook (unless cleanup was enabled)

"""
    
    markdown_content += """
---
*Report generated by Simple ERA5 Archival Flow*
"""
    
    artifact_id = create_markdown_artifact(markdown_content)
    logger.info(f"Created archival report artifact: {artifact_id}")
    
    return artifact_id


@flow(name="era5-simple-archival", description="Simple ERA5 data archival to Poseidon")
def archive_era5_simple(
    ssh_username: str,
    source_directory: Path,           # e.g., "/beegfs/CMIP6/snapdata/curated_wrf_era5-04km"
    destination_directory: Path,      # e.g., "/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km"
    variables: str = "all",           # e.g., "t2_mean,t2_min,t2_max" or "all"
    cleanup_source_archives: bool = True  # Remove tar files from Chinook after transfer
):
    """
    Simple archival flow for ERA5 data
    
    Assumes processing is already complete - no synchronization logic needed
    
    Args:
        ssh_username: Username for SSH connections to both Chinook and Poseidon
        source_directory: Directory containing ERA5 variable subdirectories on Chinook
        destination_directory: Target directory on Poseidon for archives
        variables: Comma-separated variable names or "all" for everything available
        cleanup_source_archives: Whether to remove tar files from Chinook after successful transfer
    """
    logger = get_run_logger()
    
    logger.info("🚀 Starting Simple ERA5 Archival Flow")
    logger.info(f"Source: {source_directory}")
    logger.info(f"Destination: {destination_directory}")
    logger.info(f"Variables: {variables}")
    
    # Create SSH connection to Chinook
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Connect to Chinook using local SSH agent
        logger.info("🔗 Connecting to Chinook HPC using SSH Agent...")
        ssh.connect(SSH_HOST, SSH_PORT, username=ssh_username)
        logger.info("✅ Connected to Chinook")
        
        # Step 1: Discover available variables
        logger.info("🔍 Discovering available variables...")
        available_vars = discover_available_variables(ssh, source_directory)
        
        if not available_vars:
            logger.warning("⚠️ No variables with data found in source directory")
            return {"success": [], "failed": [], "message": "No data found"}
        
        # Step 2: Parse requested variables
        if variables.lower() == "all":
            target_vars = available_vars
            logger.info(f"📋 Will archive ALL {len(target_vars)} available variables")
        else:
            target_vars = [v.strip() for v in variables.split(',') if v.strip()]
            logger.info(f"📋 Will archive {len(target_vars)} requested variables: {target_vars}")
            
            # Validate requested variables exist
            missing_vars = [v for v in target_vars if v not in available_vars]
            if missing_vars:
                logger.error(f"❌ Requested variables not found: {missing_vars}")
                logger.info(f"Available variables: {available_vars}")
                raise Exception(f"Variables not found: {missing_vars}")
        
        # Step 3: Get size estimates
        logger.info("📏 Estimating archive sizes...")
        size_estimates = []
        for var in target_vars:
            estimate = estimate_archive_size(ssh, source_directory, var)
            size_estimates.append(estimate)
            logger.info(f"📦 {var}: ~{estimate['size']} ({estimate['file_count']} files)")
        
        # Step 4: Archive each variable
        logger.info(f"🗜️ Starting archival of {len(target_vars)} variables...")
        results = {"success": [], "failed": []}
        
        for i, var in enumerate(target_vars, 1):
            logger.info(f"📦 Processing variable {i}/{len(target_vars)}: {var}")
            
            result = archive_single_variable_simple(
                ssh, source_directory, var, destination_directory, ssh_username, cleanup_source_archives
            )
            
            if result["status"] == "success":
                results["success"].append(result)
            else:
                results["failed"].append(result)
        
        # Step 5: Generate report
        logger.info("📋 Generating archival report...")
        report_id = create_simple_archive_report(results, size_estimates)
        
        # Final summary
        success_count = len(results["success"])
        failed_count = len(results["failed"])
        
        if failed_count == 0:
            logger.info(f"🎉 Archival completed successfully! All {success_count} variables archived.")
        elif success_count == 0:
            logger.error(f"❌ Archival failed completely! All {failed_count} variables failed.")
        else:
            logger.warning(f"⚠️ Partial success: {success_count} succeeded, {failed_count} failed.")
        
        logger.info(f"📋 Detailed report available in artifact: {report_id}")
        
        return {
            "success": results["success"],
            "failed": results["failed"],
            "report_artifact_id": report_id,
            "summary": f"{success_count}/{success_count + failed_count} successful"
        }
        
    except Exception as e:
        logger.error(f"❌ Archival flow failed: {str(e)}")
        raise
    finally:
        ssh.close()
        logger.info("🔗 SSH connection closed")


if __name__ == "__main__":
    archive_era5_simple.serve(
        name="era5-simple-archive-deployment",
        parameters={
            "ssh_username": "snapdata",
            "source_directory": Path("/beegfs/CMIP6/snapdata/curated_wrf_era5-04km"),
            "destination_directory": Path("/workspace/Shared/Tech_Projects/daily_wrf_downscaled_era5_4km"),
            "variables": "t2_mean,t2_min,t2_max",  # or "all"
            "cleanup_source_archives": True
        }
    ) 
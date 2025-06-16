import os
import re
import tarfile
import subprocess
from pathlib import Path
from datetime import datetime

import paramiko
from prefect import task, get_run_logger
from prefect.artifacts import create_markdown_artifact


@task
def check_for_nfs_mount(ssh, nfs_directory="/import/beegfs"):
    """
    Task to check if an NFS directory is mounted on the remote server via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - nfs_directory: Path to the NFS directory to check for
    """

    stdin, stdout, stderr = ssh.exec_command(f"df -h | grep {nfs_directory}")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def clone_github_repository(ssh, branch, destination_directory):
    """
    Task to clone a GitHub repository via SSH and switch to a specific branch if it exists.

    Parameters:
    - ssh: Paramiko SSHClient object
    - branch: Name of the branch to clone and switch to
    - destination_directory: Directory to clone the repository into
    """

    target_directory = f"{destination_directory}/wrf-downscaled-era5-curation"
    stdin, stdout, stderr = ssh.exec_command(
        f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"
    )

    directory_exists = stdout.read().decode("utf-8").strip() == "true"

    if directory_exists:
        try:
            # Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            stdin, stdout, stderr = ssh.exec_command(get_current_branch_command)
            current_branch = stdout.read().decode("utf-8").strip()
        except:
            # If the current branch cannot be determined, assume it's the wrong branch
            set_branch_to_main = f"cd {target_directory} && git checkout main"
            stdin, stdout, stderr = ssh.exec_command(set_branch_to_main)

            # Get the current branch again# Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            stdin, stdout, stderr = ssh.exec_command(get_current_branch_command)
            current_branch = stdout.read().decode("utf-8").strip()

        if current_branch != branch:
            print(f"Change repository branch to branch {branch}...")
            # If the current branch is different from the desired branch, switch to the correct branch
            switch_branch_command = f"cd {target_directory} && git checkout {branch}"
            stdin, stdout, stderr = ssh.exec_command(switch_branch_command)

        print(f"Pulling the GitHub repository on branch {branch}...")

        # Run the Git pull command to pull the repository
        git_pull_command = f"cd {target_directory} && git pull origin {branch}"
        stdin, stdout, stderr = ssh.exec_command(git_pull_command)

        # Wait for the Git command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        # Check the exit status for errors
        if exit_status != 0:
            raise Exception(
                f"Error cloning the GitHub repository. Exit status: {exit_status}"
            )
    else:
        print(f"Cloning the GitHub repository on branch {branch}...")
        # Run the Git clone command to clone the repository
        git_command = f"cd {destination_directory} && git clone -b {branch} https://github.com/ua-snap/wrf-downscaled-era5-curation.git"
        stdin, stdout, stderr = ssh.exec_command(git_command)

        # Wait for the Git command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        # Check the exit status for errors
        if exit_status != 0:
            error_output = stderr.read().decode("utf-8")
            raise Exception(
                f"Error cloning the GitHub repository. Error: {error_output}"
            )


@task
def install_conda_environment(ssh, conda_env_name, conda_env_file):
    """
    Task to check for a Python Conda environment and install it from an environment file
    if it doesn't exist on the user's account via SSH. It also checks for Miniconda installation
    and installs Miniconda if it doesn't exist.

    Parameters:
    - ssh: Paramiko SSHClient object
    - conda_env_name: Name of the Conda environment to create/install
    - conda_env_file: Path to the Conda environment file (.yml) to use for installation
    """

    # Check if the Miniconda directory exists in the user's home directory
    stdin, stdout, stderr = ssh.exec_command(
        "test -d $HOME/miniconda3 && echo 1 || echo 0"
    )

    miniconda_found = int(stdout.read())
    miniconda_installed = bool(miniconda_found)

    if not miniconda_installed:
        print("Miniconda directory not found. Installing Miniconda...")
        # Download and install Miniconda
        install_miniconda_cmd = "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash miniconda.sh -b -p $HOME/miniconda3"
        stdin, stdout, stderr = ssh.exec_command(install_miniconda_cmd)

        # Wait for the command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error_output = stderr.read().decode("utf-8")
            raise Exception(f"Error installing Miniconda. Error: {error_output}")

        print("Miniconda installed successfully")

    # Check if the Conda environment already exists
    stdin, stdout, stderr = ssh.exec_command(
        f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env list | grep {conda_env_name}"
    )

    conda_env_exists = bool(stdout.read())

    if not conda_env_exists:
        print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

        # Install the Conda environment from the environment file
        install_cmd = f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}"
        stdin, stdout, stderr = ssh.exec_command(install_cmd)

        # Wait for the command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print(f"Conda environment '{conda_env_name}' installed successfully")
        else:
            error_output = stderr.read().decode("utf-8")
            raise Exception(
                f"Error installing Conda environment '{conda_env_name}'. Error: {error_output}"
            )
    else:
        print(f"Conda environment '{conda_env_name}' already exists.")


@task
def tar_directory(directory, output_file):
    with tarfile.open(output_file, "w:gz") as tar:
        print("Creating new tar file of ERA5 data...")
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            tar.add(item_path, arcname=item)
    return output_file


@task
def copy_tarfile_to_storage_server(tar_file, target_directory):
    print("Copying tar file to NFS server via scp...")
    try:
        subprocess.run(
            ["scp", tar_file, f"poseidon.snap.uaf.edu:{target_directory}"], check=True
        )
        print(
            f"File {tar_file} successfully copied to poseidon.snap.uaf.edu:{target_directory}"
        )
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")


@task
def capture_remote_logs(ssh, repo_path: Path, log_file_path: str = None) -> dict:
    """
    Capture logs from remote HPC and create Prefect artifacts

    Args:
        ssh: Paramiko SSHClient object
        repo_path: Path to the repository on remote system
        log_file_path: Optional custom log file path

    Returns:
        dict: Status and artifact information with keys: status, content, artifact_id
    """
    logger = get_run_logger()

    # Use default log path if not provided
    if log_file_path is None:
        log_file_path = f"{repo_path}/logs/submit_era5_jobs/submit_era5_jobs.log"

    # Check if log file exists
    stdin, stdout, stderr = ssh.exec_command(
        f"test -f {log_file_path} && echo 'exists' || echo 'missing'"
    )
    file_status = stdout.read().decode("utf-8").strip()

    if file_status == "missing":
        logger.warning(f"Log file not found at {log_file_path}")
        return {"status": "missing", "content": "", "artifact_id": None}

    # Retrieve log content
    stdin, stdout, stderr = ssh.exec_command(f"cat {log_file_path}")
    log_content = stdout.read().decode("utf-8")

    logger.info(f"Retrieved {len(log_content)} characters from log file")

    return {
        "status": "success",
        "content": log_content,
        "artifact_id": None,  # Will be set by calling function
    }


@task
def parse_era5_log(log_content: str) -> dict:
    """
    Parse ERA5 log content to extract key metrics and statistics

    Args:
        log_content: Raw log file content as string

    Returns:
        dict: Parsed log data with key metrics
    """
    logger = get_run_logger()

    try:
        # Initialize result dictionary
        parsed_data = {
            "status": "UNKNOWN",
            "variables": [],
            "years_processed": {"start": None, "end": None},
            "jobs": {"submitted": 0, "completed": 0},
            "timing": {"start_time": None, "end_time": None, "duration": None},
            "retries": {"timeouts_detected": 0, "retries_attempted": 0},
            "completion_rate": 0.0,
            "validation": {
                "expected_files": 0,
                "existing_files": 0,
                "missing_files": 0,
            },
            "issues": [],
            "raw_stats": {},
        }

        lines = log_content.split("\n")

        # Extract variables from job submissions
        variables_set = set()
        years_list = []
        job_count = 0
        timeout_jobs = []
        retry_count = 0

        for line in lines:
            # Extract variables and years from job submissions
            if "Submitted job" in line and "for variable" in line:
                job_count += 1
                # Extract variable name
                var_match = re.search(r"for variable (\w+), year (\d+)", line)
                if var_match:
                    variables_set.add(var_match.group(1))
                    years_list.append(int(var_match.group(2)))

            # Extract timeout information
            elif "Timeout detected for" in line:
                timeout_match = re.search(
                    r"Timeout detected for (\w+) year (\d+)", line
                )
                if timeout_match:
                    timeout_jobs.append(
                        f"{timeout_match.group(1)} {timeout_match.group(2)}"
                    )
                    parsed_data["retries"]["timeouts_detected"] += 1

            # Extract retry job submissions
            elif "RETRY: Submitted job" in line:
                retry_count += 1
                parsed_data["retries"]["retries_attempted"] += 1

            # Extract validation results
            elif "Expected files:" in line:
                expected_match = re.search(r"Expected files: (\d+)", line)
                if expected_match:
                    parsed_data["validation"]["expected_files"] = int(
                        expected_match.group(1)
                    )

            elif "Existing files:" in line:
                existing_match = re.search(r"Existing files: (\d+)", line)
                if existing_match:
                    parsed_data["validation"]["existing_files"] = int(
                        existing_match.group(1)
                    )

            elif "Missing files:" in line:
                missing_match = re.search(r"Missing files: (\d+)", line)
                if missing_match:
                    parsed_data["validation"]["missing_files"] = int(
                        missing_match.group(1)
                    )

            # Extract completion rate
            elif "Overall completion rate:" in line:
                rate_match = re.search(r"Overall completion rate: ([\d.]+)%", line)
                if rate_match:
                    parsed_data["completion_rate"] = float(rate_match.group(1))

        # Process extracted data
        parsed_data["variables"] = sorted(list(variables_set))
        if years_list:
            parsed_data["years_processed"]["start"] = min(years_list)
            parsed_data["years_processed"]["end"] = max(years_list)

        parsed_data["jobs"]["submitted"] = job_count + retry_count
        parsed_data["jobs"]["completed"] = (
            job_count + retry_count
        )  # Assume all submitted jobs eventually complete

        # Extract timing information from first and last log entries
        timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
        timestamps = re.findall(timestamp_pattern, log_content)
        if timestamps:
            parsed_data["timing"]["start_time"] = timestamps[0]
            parsed_data["timing"]["end_time"] = timestamps[-1]

            # Calculate duration if we have both timestamps
            try:
                start_dt = datetime.strptime(timestamps[0], "%Y-%m-%d %H:%M:%S")
                end_dt = datetime.strptime(timestamps[-1], "%Y-%m-%d %H:%M:%S")
                duration = end_dt - start_dt
                parsed_data["timing"]["duration"] = str(duration)
            except ValueError:
                parsed_data["timing"]["duration"] = "Unable to calculate"

        # Determine overall status
        if "All processing completed successfully" in log_content:
            parsed_data["status"] = "SUCCESS"
        elif parsed_data["completion_rate"] >= 100.0:
            parsed_data["status"] = "SUCCESS"
        elif parsed_data["completion_rate"] >= 90.0:
            parsed_data["status"] = "PARTIAL SUCCESS"
        elif parsed_data["retries"]["timeouts_detected"] > 0:
            parsed_data["status"] = "COMPLETED WITH RETRIES"
        else:
            parsed_data["status"] = "UNKNOWN"

        # Identify issues
        if parsed_data["retries"]["timeouts_detected"] > 0:
            parsed_data["issues"].append(
                f"{parsed_data['retries']['timeouts_detected']} jobs timed out and required retries"
            )

        if parsed_data["validation"]["missing_files"] > 0:
            parsed_data["issues"].append(
                f"{parsed_data['validation']['missing_files']} expected output files are missing"
            )

        if parsed_data["completion_rate"] < 100.0:
            parsed_data["issues"].append(
                f"Completion rate is {parsed_data['completion_rate']}% (less than 100%)"
            )

        logger.info(
            f"Successfully parsed log: {len(parsed_data['variables'])} variables, {parsed_data['jobs']['submitted']} jobs"
        )
        return parsed_data

    except Exception as e:
        logger.warning(f"Error parsing log content: {str(e)}")
        return {
            "status": "PARSE_ERROR",
            "error": str(e),
            "variables": [],
            "jobs": {"submitted": 0, "completed": 0},
            "issues": [f"Log parsing failed: {str(e)}"],
        }


@task
def generate_log_summary(parsed_data: dict) -> str:
    """
    Generate a formatted summary from parsed log data

    Args:
        parsed_data: Dictionary containing parsed log metrics

    Returns:
        str: Formatted markdown summary
    """

    # Handle parse errors gracefully
    if parsed_data.get("status") == "PARSE_ERROR":
        return f"""## ⚠️ Log Parsing Error

**Error**: {parsed_data.get('error', 'Unknown parsing error')}

*Unable to generate detailed summary. Please review the full log below.*

---
"""

    # Build variables string
    variables_str = (
        ", ".join(parsed_data["variables"]) if parsed_data["variables"] else "Unknown"
    )

    # Build year range string
    year_start = parsed_data["years_processed"].get("start")
    year_end = parsed_data["years_processed"].get("end")
    if year_start and year_end:
        if year_start == year_end:
            years_str = str(year_start)
        else:
            years_str = f"{year_start} - {year_end}"
    else:
        years_str = "Unknown"

    # Status emoji mapping
    status_emoji = {
        "SUCCESS": "✅",
        "PARTIAL SUCCESS": "⚠️",
        "COMPLETED WITH RETRIES": "🔄",
        "UNKNOWN": "❓",
        "FAILED": "❌",
    }

    status_icon = status_emoji.get(parsed_data["status"], "❓")

    # Build duration string
    duration = parsed_data["timing"].get("duration", "Unknown")

    # Calculate success rate for display
    completion_rate = parsed_data.get("completion_rate", 0)

    # Build summary
    summary = f"""## 📊 ERA5 Processing Executive Summary

### 🎯 Quick Overview
| Metric | Value |
|--------|-------|
| **Status** | {status_icon} {parsed_data["status"]} |
| **Variables** | {variables_str} |
| **Years Processed** | {years_str} |
| **Total Jobs** | {parsed_data["jobs"]["submitted"]} submitted |
| **Duration** | {duration} |

### 📈 Performance Metrics
| Metric | Value |
|--------|-------|
| **Success Rate** | {completion_rate}% |
| **Retries Required** | {parsed_data["retries"]["retries_attempted"]} jobs |
| **Timeouts Detected** | {parsed_data["retries"]["timeouts_detected"]} jobs |
"""

    # Add validation section if data available
    if parsed_data["validation"]["expected_files"] > 0:
        summary += f"""| **File Validation** | {parsed_data["validation"]["existing_files"]}/{parsed_data["validation"]["expected_files"]} files present |
"""

    # Add timing details if available
    if parsed_data["timing"]["start_time"]:
        summary += f"""
### ⏱️ Execution Timeline
| Metric | Value |
|--------|-------|
| **Started** | {parsed_data["timing"]["start_time"]} |
| **Completed** | {parsed_data["timing"]["end_time"]} |
| **Total Duration** | {duration} |
"""

    # Add issues section if any exist
    if parsed_data["issues"]:
        summary += f"""
### ⚠️ Issues & Warnings
"""
        for issue in parsed_data["issues"]:
            summary += f"- {issue}\n"
    else:
        summary += f"""
### ✅ No Issues Detected
All processing completed without warnings or errors.
"""

    summary += "\n---\n"

    return summary


@task
def create_full_log_artifact(ssh, repo_path: Path) -> str:
    """Create complete log file artifact with intelligent summary"""
    logger = get_run_logger()

    log_result = capture_remote_logs(ssh, repo_path)

    if log_result["status"] == "missing":
        markdown_content = f"""# 📝 ERA5 Full Log

**Status:** Log file not found  
**Repository:** `{repo_path}`  
**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

No log file was found at the expected location.
"""
    else:
        log_content = log_result["content"]

        # Parse log content and generate summary
        try:
            parsed_data = parse_era5_log(log_content)
            summary = generate_log_summary(parsed_data)
            logger.info("Successfully generated log summary")
        except Exception as e:
            logger.warning(f"Failed to generate summary: {str(e)}")
            summary = f"""## ⚠️ Summary Generation Error

Unable to generate automated summary: {str(e)}

---
"""

        # Truncate extremely large logs for the full content section
        display_log_content = log_content
        if len(log_content) > 100000:  # 100KB limit for full log
            display_log_content = (
                log_content[:100000]
                + "\n\n... (log truncated - showing first 100KB) ..."
            )

        markdown_content = f"""# 📝 ERA5 Full Execution Log

**Repository:** `{repo_path}`  
**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Size:** {len(log_result["content"])} characters

{summary}

## 📋 Complete Log Output

```bash
{display_log_content}
```

---
*Complete log captured by ERA5 Curation Flow*
"""

    artifact_id = create_markdown_artifact(markdown_content)
    logger.info(f"Created enhanced log artifact with summary: {artifact_id}")
    return artifact_id

"""
Run the NG-IDF curation pipeline (ua-snap/ng-idf_curation) on Chinook and document every stage.

The science and the checks live in the curation repo (`ngidf/pipeline.py`). This flow only drives it
over SSH:

  1. clone or update the curation repo at the requested branch (or commit);
  2. `pipeline.py info` and `plan`: the model version and every stage's state. A stage whose
     fingerprint (its code, inputs and settings) already has a passing result is reused, not re-run;
  3. submit every stage that is ready (independent stages run side by side on Slurm), wait for the
     jobs, `finalize` (the stage's checks -> report.json); repeat until nothing more can run;
  4. one Prefect artifact per stage (verdict, checks, measurements, Slurm accounting, what the
     fingerprint covers), a plan artifact at the start, a summary artifact at the end;
  5. optionally cut a release (`pipeline.py release`) when every stage has passed.

A new model version is a new file in the curation repo's config/model_versions/ (see its README);
pass its label as `model_version`. Only the stages that read the model run again.
"""

import json
import shlex
import time
from pathlib import Path

import paramiko
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact

from utils import utils
import ngidf_artifacts as art

SSH_HOST = "chinook04.rcs.alaska.edu"
SSH_PORT = 22
REPO_NAME = "ng-idf_curation"
# non-interactive SSH shells do not read the login profile; it puts sbatch/squeue/sacct on PATH
PREAMBLE = ". /etc/profile >/dev/null 2>&1; "


class Pipeline:
    """`python -I ngidf/pipeline.py ...` in the clone on Chinook; every command prints one JSON document."""

    def __init__(self, ssh, repo_dir, python, model_version="", data_root=""):
        self.ssh, self.repo, self.python = ssh, str(repo_dir), python
        self.globals = []
        if model_version:
            self.globals += ["--model-version", model_version]
        if data_root:
            self.globals += ["--data-root", data_root]

    def command(self, *args):
        argv = " ".join(shlex.quote(a) for a in [*self.globals, *args])
        # python is not quoted so that $HOME in it expands
        return f"{PREAMBLE}cd {shlex.quote(self.repo)} && {self.python} -I ngidf/pipeline.py {argv}"

    def run(self, *args, allow_fail=False):
        status, out, err = utils.exec_command(self.ssh, self.command(*args))
        doc = None
        if out:
            try:
                doc = json.loads(out)
            except json.JSONDecodeError:
                pass
        if (status != 0 and not allow_fail) or doc is None:
            raise RuntimeError(f"pipeline.py {' '.join(args)} failed (exit {status}):\n{err[-3000:] or out[-3000:]}")
        return doc, status, err

    def sh(self, cmd):
        return utils.exec_command(self.ssh, PREAMBLE + cmd)

    def read_json(self, path):
        status, out, err = self.sh(f"cat {shlex.quote(str(path))}")
        if status != 0:
            raise RuntimeError(f"cannot read {path}: {err}")
        return json.loads(out)


# ------------------------------------------------------------------------------------- planning
def decide(plan, stop_before="", retry_failed=False):
    """What this flow run does with each stage."""
    order = [s["stage"] for s in plan]
    if stop_before and stop_before not in order:
        raise ValueError(f"stop_before={stop_before!r} is not a stage; stages are {', '.join(order)}")
    cut = order.index(stop_before) if stop_before else len(order)
    actions = {}
    for i, s in enumerate(plan):
        st = s["status"]
        if st in ("pass", "warn"):
            actions[s["stage"]] = "reuse"
        elif i >= cut:
            actions[s["stage"]] = "exclude"
        elif st == "waiting":
            actions[s["stage"]] = "wait"
        elif st == "prepared":
            # submitted by an earlier flow run that did not finish: wait for its jobs; with no jobs
            # recorded the attempt died before submitting, so it is moved aside and run again
            actions[s["stage"]] = "resume" if s.get("job_ids") else "retry"
        elif st == "fail":
            actions[s["stage"]] = "retry" if retry_failed else "skip-failed"
        else:
            actions[s["stage"]] = "run"
    return actions


# ----------------------------------------------------------------------------------------- tasks
@task(task_run_name="clone-curation-repo")
def prepare_repo(ssh, branch_name, working_directory, curation_commit):
    """Clone the curation repo, or update the flow's existing clone, and check out the branch head
    (or an exact commit). The clone belongs to the flow: it refuses to touch one with local edits.
    Done here rather than with utils.clone_github_repository so git's own error reaches the log."""
    q = shlex.quote
    repo = f"{working_directory}/{REPO_NAME}"
    checkout = (f"git checkout -q --detach {q(curation_commit)}" if curation_commit
                else f"git checkout -q -B {q(branch_name)} origin/{q(branch_name)}")
    cmd = (f"set -e; export GIT_TERMINAL_PROMPT=0; mkdir -p {q(str(working_directory))}; "
           f"if [ ! -d {q(repo)}/.git ]; then git clone -q https://github.com/ua-snap/{REPO_NAME}.git {q(repo)}; fi; "
           f"cd {q(repo)}; "
           f"if [ -n \"$(git status --porcelain --untracked-files=no)\" ]; then "
           f"echo 'the clone has local edits; it belongs to the flow, so commit or discard them' >&2; exit 3; fi; "
           f"git fetch -q --prune origin; "
           + ("" if curation_commit else
              f"if ! git rev-parse -q --verify origin/{q(branch_name)} >/dev/null; then "
              f"echo \"ua-snap/{REPO_NAME} has no branch {q(branch_name)} (branch_name names a branch of the "
              f"curation repo, not of this prefect repo). Its branches: $(git branch -r --format='%(refname:lstrip=3)' "
              f"| grep -v '^HEAD$' | tr '\\n' ' ')\" >&2; exit 4; fi; ")
           + f"{checkout}; git log -1 --format='%H %s'")
    status, out, err = utils.exec_command(ssh, cmd)
    if status != 0:
        raise RuntimeError(f"preparing the curation repo at {repo} failed (exit {status}):\n{err or out}")
    get_run_logger().info(f"{repo} at {out}")
    return repo


@task(task_run_name="check-access-to-the-model")
def check_app_access(pipe, model):
    """The app stage downloads the model from the SNAP mirror; fail early and clearly if the Chinook
    account cannot read it or the tag is missing."""
    repo, ref = model["APP_REPO"], model["APP_REF"]
    # an annotated tag's ref points at the tag object; the peeled ref (^{}) is the commit. A pattern
    # does not return the peeled line unless asked for it by name.
    status, out, err = pipe.sh(f"GIT_TERMINAL_PROMPT=0 git ls-remote {shlex.quote(repo)} "
                               + " ".join(shlex.quote(r) for r in (f"refs/tags/{ref}", f"refs/tags/{ref}^{{}}",
                                                                   f"refs/heads/{ref}")))
    if status != 0 or not out:
        raise RuntimeError(f"cannot read {ref} from {repo} on Chinook ({err.strip() or 'ref not found'}). "
                           "The Chinook account needs read access to the mirror, and the tag must be pushed "
                           "(curation repo README, 'A new model version').")
    refs = dict(reversed(line.split("\t", 1)) for line in out.splitlines() if "\t" in line)
    sha = refs.get(f"refs/tags/{ref}^{{}}") or refs.get(f"refs/tags/{ref}") or refs.get(f"refs/heads/{ref}")
    if sha != model["APP_COMMIT"]:
        raise RuntimeError(f"{repo} {ref} is {sha}, but the model version file says {model['APP_COMMIT']}")
    get_run_logger().info(f"{repo} {ref} -> {sha} (matches the model version file)")


@task(task_run_name="submit-{stage}")
def submit_stage(pipe, stage, retire_first=False):
    log = get_run_logger()
    if retire_first:
        doc, _, _ = pipe.run("retire", "--stage", stage)
        log.info(f"{stage}: earlier attempt moved to {doc.get('retired')}")
    try:
        doc, status, err = pipe.run("submit", "--stage", stage, allow_fail=True)
    except RuntimeError as e:   # sbatch refused, or a login-node step failed before finishing
        log.error(f"{stage}: submission failed\n{e}")
        return dict(stage=stage, job_ids=[], finalized=False, submit_error=str(e))
    for line in err.splitlines():
        log.info(line)
    return doc


@task(task_run_name="finalize-{stage}")
def finalize_stage(pipe, stage, stage_dir, job_ids):
    """Run the stage's checks (report.json) and return the report."""
    log = get_run_logger()
    args = ["finalize", "--stage", stage] + (["--job-ids", *job_ids] if job_ids else [])
    try:
        pipe.run(*args, allow_fail=True)
    except RuntimeError as e:
        log.error(f"{stage}: finalize failed\n{e}")
        log_tail(pipe, stage_dir)
        return None
    return read_report(pipe, stage_dir)


def read_report(pipe, stage_dir):
    report = pipe.read_json(Path(stage_dir) / "report.json")
    if report["status"] == "fail":
        log_tail(pipe, stage_dir)
    return report


def log_tail(pipe, stage_dir, files=3, lines=40):
    """The end of the newest job logs of a failed stage, into the flow log."""
    log = get_run_logger()
    d = shlex.quote(f"{stage_dir}/logs")
    status, out, _ = pipe.sh(f"ls -t {d} 2>/dev/null | head -n {files}")
    for name in out.split():
        _, text, _ = pipe.sh(f"tail -n {lines} {d}/{shlex.quote(name)}")
        log.error(f"--- {stage_dir}/logs/{name} (last {lines} lines)\n{text}")


def squeue_alive(pipe):
    status, out, _ = pipe.sh("squeue -h -o %A -u $(whoami)")
    return set(out.split()) if status == 0 else None


def sacct_rows(pipe, job_ids):
    """Slurm accounting for a stage's jobs, one row per job and state (array tasks counted)."""
    if not job_ids:
        return []
    status, out, _ = pipe.sh(f"sacct -X -n -P -o JobID,JobName,State,ExitCode,Elapsed -j {','.join(job_ids)}")
    if status != 0:
        return []
    groups = {}
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) < 5:
            continue
        jid, name, state, code, elapsed = parts[:5]
        key = (jid.split("_")[0], name, state.split()[0])
        g = groups.setdefault(key, dict(job=key[0], name=name, state=key[2], tasks=0, exit_codes=set(), longest=""))
        g["tasks"] += 1
        g["exit_codes"].add(code)
        g["longest"] = max(g["longest"], elapsed)
    return [dict(g, exit_codes=" ".join(sorted(g["exit_codes"]))) for g in groups.values()]


@task(task_run_name="document-{stage}")
def publish_stage(stage, report, ran_now, slurm_jobs, upstream):
    md = art.stage_markdown(report, ran_now, slurm_jobs, upstream)
    create_markdown_artifact(key=art.artifact_key("ngidf", stage), markdown=md,
                             description=f"{report['title']}: {art.label(report['status'])}")
    for key, (rows, desc) in art.big_tables(report).items():
        create_table_artifact(key=key, table=rows, description=desc)


# ------------------------------------------------------------------------------------------- flow
@flow(
    name="ng-idf-pipeline",
    description="Run the NG-IDF curation pipeline on Chinook, reusing unchanged stages, and document every check",
    log_prints=True,
)
def ngidf_pipeline(
    ssh_username: str,
    ssh_private_key_path: Path,
    branch_name: str = "main",
    working_directory: Path = Path("/import/beegfs/SNAP/jdpaul3"),
    model_version: str = "",
    data_root: str = "",
    curation_commit: str = "",
    stop_before: str = "",
    retry_failed: bool = False,
    release_name: str = "",
    poll_seconds: int = 120,
    driver_python: str = "$HOME/miniconda3/bin/python3",
):
    """
    Args:
        ssh_username: Chinook account that owns the data directory.
        ssh_private_key_path: Private key for that account.
        branch_name: Branch of the curation repo, ua-snap/ng-idf_curation, to run (not a branch of this
            prefect repo).
        working_directory: Where the curation repo is cloned on Chinook.
        model_version: Label of a file in config/model_versions/ (empty: the repo's CURRENT).
        data_root: Override DATA_ROOT from config/paths.env (empty: keep it).
        curation_commit: Run this exact commit instead of the branch head (empty: branch head).
        stop_before: Do not run this stage or any after it (e.g. "runs" to check the reference
            runs before spending the ~6,700 production runs). Empty: run everything.
        retry_failed: Run failed stages again. The failed attempt is moved aside, not deleted.
        release_name: Cut a release with this name when every stage has passed (empty: no release).
        poll_seconds: How often to check the Slurm queue.
        driver_python: Python >= 3.9 on Chinook to run pipeline.py (stdlib only).
    """
    log = get_run_logger()
    params = dict(branch_name=branch_name, model_version=model_version or "(CURRENT)", data_root=data_root or "(paths.env)",
                  curation_commit=curation_commit or "(branch head)", stop_before=stop_before or "(none)",
                  retry_failed=retry_failed, release_name=release_name or "(none)")
    # connect here rather than with utils.connect_ssh, which older installs of the shared utils lack
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SSH_HOST, SSH_PORT, ssh_username, pkey=paramiko.RSAKey(filename=str(ssh_private_key_path)))
    ssh.get_transport().set_keepalive(60)   # the waits last hours
    try:
        utils.check_for_nfs_mount(ssh, "/import/beegfs")
        repo = prepare_repo(ssh, branch_name, working_directory, curation_commit)
        pipe = Pipeline(ssh, repo, driver_python, model_version, data_root)

        info = pipe.run("info")[0]
        if info["curation_dirty"]:
            log.warning("The clone on Chinook has uncommitted changes; every stage that runs will carry a warning.")
        plan = pipe.run("plan")[0]
        actions = decide(plan, stop_before, retry_failed)
        upstream = {s["fingerprint"]: s["stage"] for s in plan if s.get("fingerprint")}
        reports, ran_now = {}, set()
        # read the existing results first, so the plan can say why each stage has its status
        for s in plan:
            if actions[s["stage"]] in ("reuse", "skip-failed"):
                reports[s["stage"]] = read_report(pipe, s["dir"])
        create_markdown_artifact(key="ngidf-plan", markdown=art.plan_markdown(info, plan, actions, params, reports),
                                 description=f"Plan for model {info['model']['MODEL_LABEL']}")
        if actions.get("app") in ("run", "retry"):
            check_app_access(pipe, info["model"])
        # document the reused stages too: the flow run's artifacts are the complete record
        for name, rep in list(reports.items()):
            publish_stage(name, rep, False, [], upstream)

        attempted = set()
        while True:
            plan = pipe.run("plan")[0]
            upstream.update({s["fingerprint"]: s["stage"] for s in plan if s.get("fingerprint")})
            actions = decide(plan, stop_before, retry_failed)
            ready = [s for s in plan if actions[s["stage"]] in ("run", "retry", "resume") and s["stage"] not in attempted]
            if not ready:
                break
            log.info("Ready: " + ", ".join(s["stage"] for s in ready))
            inflight = {}
            for s in ready:
                name = s["stage"]
                attempted.add(name)
                if actions[name] == "resume":
                    log.info(f"{name}: waiting for jobs submitted by an earlier flow run: {s['job_ids']}")
                    inflight[name] = (s["dir"], s["job_ids"])
                    continue
                res = submit_stage(pipe, name, retire_first=actions[name] == "retry")
                if res.get("finalized"):      # login-node stage: done when submit returns
                    ran_now.add(name)
                    reports[name] = read_report(pipe, res["dir"])
                    publish_stage(name, reports[name], True, [], upstream)
                elif res.get("submit_error"):
                    ran_now.add(name)
                    rep = finalize_stage(pipe, name, s["dir"], [])   # records the failure with the checks that can run
                    if rep:
                        reports[name] = rep
                        publish_stage(name, rep, True, [], upstream)
                else:
                    log.info(f"{name}: Slurm jobs {', '.join(res['job_ids'])}")
                    inflight[name] = (res["dir"], res["job_ids"])

            # finalize each stage as soon as its jobs have left the queue
            while inflight:
                alive = squeue_alive(pipe)
                if alive is not None:
                    for name in [n for n, (_, ids) in inflight.items() if not set(ids) & alive]:
                        stage_dir, ids = inflight.pop(name)
                        ran_now.add(name)
                        rep = finalize_stage(pipe, name, stage_dir, ids)
                        if rep:
                            reports[name] = rep
                            publish_stage(name, rep, True, sacct_rows(pipe, ids), upstream)
                            log.info(f"{name}: {art.label(rep['status'])} ({art.headline(rep)})")
                if inflight:
                    log.info(f"Waiting on {len(inflight)} stage(s): {', '.join(inflight)}")
                    time.sleep(poll_seconds)

        plan = pipe.run("plan")[0]
        actions = decide(plan, stop_before, retry_failed)
        excluded = {n for n, a in actions.items() if a == "exclude"}
        relevant = [s["status"] for s in plan if s["stage"] not in excluded]
        overall = art.overall_status(relevant)

        release = None
        if release_name:
            if excluded or overall not in ("pass", "warn"):
                log.warning(f"No release: the pipeline is not complete ({overall}).")
            else:
                release = pipe.run("release", "--name", release_name)[0]
                release["dir"] = f"{info['data_root']}/releases/{release_name}"
                log.info(f"Released {release_name} in {release['dir']}")

        create_markdown_artifact(key="ngidf-summary",
                                 markdown=art.summary_markdown(info, plan, reports, ran_now, release, excluded),
                                 description=f"NG-IDF pipeline, model {info['model']['MODEL_LABEL']}: {art.label(overall)}")
        if overall == "fail" or overall == "waiting":
            stuck = [s["stage"] for s in plan if s["stage"] not in excluded and s["status"] not in ("pass", "warn")]
            raise RuntimeError(f"Pipeline incomplete: {', '.join(stuck)} did not pass (see the ngidf-summary artifact)")
        log.info(f"Pipeline {art.label(overall)}")
        return overall
    finally:
        ssh.close()


if __name__ == "__main__":
    ngidf_pipeline.serve(
        name="ng-idf-pipeline",
        parameters={
            "ssh_username": "jdpaul3",
            "ssh_private_key_path": "/path/to/.ssh/id_rsa",
            "branch_name": "main",
            "working_directory": Path("/import/beegfs/SNAP/jdpaul3"),
            "model_version": "",
            "stop_before": "",
            "retry_failed": False,
            "release_name": "",
        },
    )

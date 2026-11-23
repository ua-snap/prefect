"""Markdown for the NG-IDF flow's Prefect artifacts, built from the curation pipeline's JSON.

Pure functions, no Prefect import, so they can be tested without a server (test_ngidf_artifacts.py).
The inputs are what `ngidf/pipeline.py info|plan` print and each stage's report.json.

Readers: SNAP colleagues (who want to know whether the run is good, in plain words) and the dataset's
author (who wants the evidence). So every artifact leads with the verdict, then the checks, then
the detail; status is always an icon *and* a word.
"""
import re

STATUS = {
    "pass": ("✅", "Passed"),
    "warn": ("⚠️", "Passed with warnings"),
    "fail": ("❌", "Failed"),
    "waiting": ("⏳", "Waiting for an earlier stage"),
    "todo": ("▫️", "Not run yet"),
    "prepared": ("🔄", "Submitted, not finished"),
    "submitted": ("🔄", "Submitted"),
    "excluded": ("⏸️", "Not run (stopped before this stage)"),
}
CHECK = {"pass": "✅ Pass", "warn": "⚠️ Warning", "fail": "❌ Fail"}
HEX = re.compile(r"^[0-9a-f]{40,64}$")
INLINE_ROWS, INLINE_COLS = 25, 8
FINGERPRINT = """\
A **fingerprint** is a code (a SHA-256 hash) calculated from everything a stage uses to make its outputs:

- the exact contents of its scripts;
- the contents of its input files, or the fingerprints of the earlier stages whose outputs it reads;
- its settings (which places, which model commit, ...);
- the fingerprint of the software environment it runs in.

Change any of these (one character of a script, one row of an input table, a new model version) and the
fingerprint changes; otherwise it stays the same. Each stage's outputs live in a folder named after its
fingerprint (`stages/<stage>/<first 16 characters>/`). If that folder already holds a passing result, the stage
is **reused**. Otherwise it runs into a new folder, so earlier results are never overwritten. Because a stage's
fingerprint includes those of the stages it reads from, a change re-runs exactly the stages downstream of it
and no others.

Left out, because they cannot change the results: where the code is checked out, who ran it and when, and the
checks themselves (they can be run again on existing outputs). One shortcut: the 86 TB WRF/CESM archive is
identified by its path rather than by hashing its contents. It is SNAP's read-only archive, and the `inputs`
stage records its file counts and sizes.
"""

LEGEND = (
    "**Key:** ✅ **Passed**: every check passed. "
    "⚠️ **Passed with warnings**: the outputs passed every check that decides whether they can be used, but at "
    "least one check flagged something a person should know; each warning is listed below the table. "
    "❌ **Failed**: a check that decides whether the outputs can be used did not pass; stages that depend on it "
    "do not run.")
ADOPTED_WHY = (
    "These stages use outputs made on 2026-11-17, before the pipeline existed, rather than recomputing them (the "
    "forcing alone is about 30 TB of reads). Every check was run on them again; the warning only records that the "
    "pipeline did not make them itself. It goes away for any stage the pipeline runs itself.")

# warnings every stage carries for the same reason; listed once in the summary, not per stage
ROUTINE_WARNINGS = ("outputs produced by this pipeline (not adopted)", "curation code committed")


def icon(status):
    return STATUS.get(status, ("•", status))[0]


def label(status):
    i, w = STATUS.get(status, ("•", status))
    return f"{i} {w}"


def artifact_key(*parts):
    """Prefect artifact keys: lowercase letters, numbers and dashes only."""
    return re.sub(r"[^a-z0-9-]+", "-", "-".join(str(p) for p in parts).lower()).strip("-")


def cell(x, width=160):
    """One markdown table cell: no pipes or newlines, long text cut."""
    s = "" if x is None else str(x)
    s = s.replace("|", "\\|").replace("\n", " ").strip()
    return s if len(s) <= width else s[: width - 1] + "…"


def short(v):
    """Hashes shortened for reading; everything else as is."""
    s = str(v)
    return f"`{s[:12]}…`" if HEX.match(s) else f"`{cell(s, 100)}`"


def md_table(rows, columns=None):
    if not rows:
        return "_(empty)_"
    columns = columns or list(rows[0])
    out = ["| " + " | ".join(cell(c) for c in columns) + " |", "|" + "---|" * len(columns)]
    for r in rows:
        out.append("| " + " | ".join(cell(fmt_value(r.get(c))) for c in columns) + " |")
    return "\n".join(out)


def fmt_value(v):
    if isinstance(v, float):
        return f"{v:.4g}"
    if isinstance(v, list):
        return ", ".join(fmt_value(x) for x in v[:6]) + (" …" if len(v) > 6 else "")
    return v


def check_counts(checks):
    n = {"pass": 0, "warn": 0, "fail": 0}
    for c in checks:
        n[c["status"]] = n.get(c["status"], 0) + 1
    return n


def headline(report):
    """The one check a reader should see first: the first failure, else the first warning that is
    about the science rather than bookkeeping, else 'all N checks passed'."""
    checks = report.get("checks", [])
    for status in ("fail", "warn"):
        real = [c for c in checks if c["status"] == status and c["name"] not in ROUTINE_WARNINGS]
        if real:
            c = real[0]
            return f"{c['name']}: {c['detail']}" if c.get("detail") else c["name"]
    if not checks:
        return "no checks"
    science = [c for c in checks if c["name"] not in ROUTINE_WARNINGS]
    notes = []
    for c in checks:
        if c["name"] in ROUTINE_WARNINGS and c["status"] != "pass":
            notes.append("outputs adopted from earlier work" if c["name"].startswith("outputs produced")
                         else "code not committed")
    if not notes:
        return f"all {len(checks)} checks passed"
    return f"all {len(science)} checks on the outputs passed ({'; '.join(notes)})"


# --------------------------------------------------------------------------------- run plan
def compact_needs(needs):
    forcing = [n for n in needs if n.startswith("forcing_")]
    rest = [n for n in needs if not n.startswith("forcing_")]
    return ", ".join(rest + ([f"{len(forcing)} forcing stages"] if len(forcing) > 2 else forcing)) or "–"


def plan_markdown(info, plan, actions, flow_params, reports=None):
    """Artifact at the start of a flow run: what will run, what is reused, and why.
    reports: the report.json of each stage that already has one, for the 'why' of its status."""
    reports = reports or {}
    m = info["model"]
    commit = info["curation_commit"] or "?"
    remote = info.get("curation_remote", "").removesuffix(".git")
    n = {a: sum(1 for x in actions.values() if x == a) for a in ("run", "resume", "reuse", "wait", "exclude", "retry")}
    L = [
        f"# NG-IDF pipeline: plan for model `{m['MODEL_LABEL']}`",
        "",
        "This flow runs SNAP's NG-IDF curation pipeline on Chinook: 25 stages, from the software environments "
        "to the final quality report. A stage runs only if something it uses has changed since it last passed; "
        "otherwise its earlier result is reused. Whether anything changed is decided by the stage's fingerprint "
        "(below).",
        "",
        f"**This run:** {n['run'] + n['retry']} stages to run, {n['resume']} to resume, {n['reuse']} reused unchanged, "
        f"{n['wait']} waiting on earlier stages" + (f", {n['exclude']} excluded by `stop_before`" if n["exclude"] else "") + ".",
        "",
        "## What a fingerprint is",
        "",
        FINGERPRINT,
        "## Model version",
        "",
        "| | |",
        "|---|---|",
        f"| Label | `{m['MODEL_LABEL']}` |",
        f"| Description | {cell(m.get('MODEL_DESCRIPTION', ''), 300)} |",
        f"| Author's repository | {m.get('UPSTREAM_REPO', '')} (branch `{m.get('UPSTREAM_BRANCH', '')}`) |",
        f"| Commit | `{m.get('APP_COMMIT', '')}` |",
        f"| SNAP mirror (what the pipeline downloads) | {m.get('APP_REPO', '')} tag `{m.get('APP_REF', '')}` |",
        f"| Compared against | {('model ' + m['BASELINE_MODEL']) if m.get('BASELINE_MODEL') else ('coverage in ' + m.get('BASELINE_COVERAGE_DIR', '') if m.get('BASELINE_COVERAGE_DIR') else 'nothing')} |",
        "",
        "## Curation code",
        "",
        "| | |",
        "|---|---|",
        f"| Commit | [`{commit[:7]}`]({remote}/commit/{commit}) on branch `{info.get('curation_branch', '')}` |",
        f"| Uncommitted changes | {'**yes** (results will be marked)' if info.get('curation_dirty') else 'none'} |",
        f"| Data directory | `{info['data_root']}` |",
        f"| Slurm | partition `{info['slurm']['partition']}`, QOS `{info['slurm']['qos']}` |",
        "",
        "## Stages",
        "",
        LEGEND,
        "",
        "| Stage | Now | Why | This run | Needs |",
        "|---|---|---|---|---|",
    ]
    verb = {"run": "▶️ run", "retry": "▶️ run again (earlier attempt kept)", "resume": "🔄 wait for jobs already submitted",
            "reuse": "♻️ reuse", "wait": "⏳ after its inputs", "exclude": "⏸️ not this run", "skip-failed": "❌ left failed"}
    for s in plan:
        r = reports.get(s["stage"])
        why = headline(r) if r else s.get("reason", "")
        L.append(f"| **{s['stage']}** {cell(s.get('title', ''))} | {label(s['status'])} | {cell(why, 200)} "
                 f"| {verb.get(actions[s['stage']], actions[s['stage']])} | {compact_needs(s.get('needs', []))} |")
    L += warnings_md(reports)
    if flow_params:
        L += ["", "## Flow parameters", "", md_table([dict(parameter=k, value=v) for k, v in flow_params.items()])]
    return "\n".join(L) + "\n"


# ----------------------------------------------------------------------------------- stage
def stage_markdown(report, ran_now, slurm_jobs=None, upstream=None):
    """Artifact for one stage.

    report: the stage's report.json; ran_now: True if this flow run produced it (else reused);
    slurm_jobs: rows from sacct for the jobs of this run; upstream: {fingerprint: stage name} to
    name the stages a fingerprint depends on.
    """
    upstream = upstream or {}
    st = report["status"]
    n = check_counts(report.get("checks", []))
    L = [f"# {icon(st)} {report['title']}", "", report.get("explain", ""), ""]
    L += ["| | |", "|---|---|",
          f"| Result | **{label(st)}**: {n['pass']} passed, {n['warn']} warnings, {n['fail']} failed |",
          f"| In this flow run | {'ran now' if ran_now else 'reused: inputs and code unchanged since it ran'} (finished {report.get('finalized', '?')}) |",
          f"| Model | `{report.get('model_label')}` (app commit `{(report.get('app_commit') or '')[:12]}`) |",
          f"| Curation code | `{(report.get('curation_commit') or '?')[:7]}` |",
          f"| Fingerprint | `{report['fingerprint'][:16]}`: the ID of exactly what this stage used (listed at the end); also its folder name |",
          f"| Directory | `{report['dir']}` |"]
    if report.get("job_ids"):
        L.append(f"| Slurm jobs | {', '.join(report['job_ids'])} |")
    if report.get("adopted"):
        L.append("| Outputs | adopted from work done before the pipeline existed, then checked again (see the first check) |")
    L += ["", "## Checks", "", "| Result | Check | Detail |", "|---|---|---|"]
    for c in report.get("checks", []):
        L.append(f"| {CHECK.get(c['status'], c['status'])} | {cell(c['name'])} | {cell(c.get('detail', ''), 400)} |")
    if report.get("metrics"):
        L += ["", "## Measurements", ""]
        L += [f"- **{k}**: {fmt_value(v)}" for k, v in report["metrics"].items()]
    if report.get("summary_md"):
        L += ["", "## Details", "", report["summary_md"].strip()]
    big = []
    for name, rows in (report.get("tables") or {}).items():
        if not rows:
            continue
        if len(rows) <= INLINE_ROWS and len(rows[0]) <= INLINE_COLS:
            L += ["", f"## Table: {name}", "", md_table(rows)]
        else:
            big.append(name)
    if big:
        L += ["", "Larger tables are in separate table artifacts: " + ", ".join(
            f"`{artifact_key('ngidf', report['stage'], t)}`" for t in big) + "."]
    if slurm_jobs:
        L += ["", "## Slurm accounting", "", md_table(slurm_jobs)]
    comp = report.get("components") or {}
    if comp:
        L += ["", "## What the fingerprint covers", "",
              "The fingerprint is calculated from the items below. The stage runs again only if one of them "
              "changes. Long codes are SHA-256 hashes of file contents, shortened here; a code marked with a stage "
              "name is that stage's fingerprint. Full definition: the `ngidf-plan` artifact.", "",
              "**Code files**", ""]
        L += [f"- `{f}` {short(h)}" for f, h in (comp.get("code") or {}).items()]
        L += ["", "**Inputs and settings**", ""]
        for k, v in (comp.get("inputs") or {}).items():
            if isinstance(v, dict):
                L.append(f"- **{k}**: " + ", ".join(f"{a}={short(b)}" for a, b in v.items()))
            elif isinstance(v, list):
                L.append(f"- **{k}**: " + ", ".join(short(x) for x in v[:8]) + (f" … ({len(v)} items)" if len(v) > 8 else ""))
            else:
                who = f" (stage **{upstream[v]}**)" if v in upstream else ""
                L.append(f"- **{k}**: {short(v)}{who}")
    return "\n".join(L) + "\n"


def big_tables(report):
    """{artifact key: (rows, description)} for tables too large to inline."""
    out = {}
    for name, rows in (report.get("tables") or {}).items():
        if rows and not (len(rows) <= INLINE_ROWS and len(rows[0]) <= INLINE_COLS):
            out[artifact_key("ngidf", report["stage"], name)] = (
                [{k: fmt_value(v) for k, v in r.items()} for r in rows],
                f"{report['title']}: table '{name}' ({len(rows)} rows)")
    return out


# --------------------------------------------------------------------------------- summary
def overall_status(statuses):
    if any(s == "fail" for s in statuses):
        return "fail"
    if any(s not in ("pass", "warn") for s in statuses):
        return "waiting"
    return "warn" if any(s == "warn" for s in statuses) else "pass"


def summary_markdown(info, plan, reports, ran_now, release=None, excluded=()):
    """Artifact at the end of a flow run: one verdict, one row per stage, the warnings in one list,
    and where the report is."""
    m = info["model"]
    statuses = [s["status"] for s in plan if s["stage"] not in excluded]
    ov = overall_status(statuses)
    ran = [s for s in plan if s["stage"] in ran_now]
    L = [f"# {icon(ov)} NG-IDF pipeline, model `{m['MODEL_LABEL']}`: {label(ov).split(' ', 1)[1]}", ""]
    verdict = {
        "pass": "Every stage passed every check.",
        "warn": "Every stage finished and its outputs passed the checks that decide whether results are usable; "
                "some checks raised warnings, listed below, which a person should read.",
        "fail": "At least one stage failed. Its outputs are kept for inspection; stages that depend on it did not run.",
        "waiting": "Not every stage has run yet (see the table).",
    }[ov]
    L += [verdict, "",
          f"{len(ran)} of {len(plan)} stages ran in this flow run. The others were reused: their fingerprints "
          "(the code, inputs and settings they use) match a result that already passed. See `ngidf-plan` for "
          "what a fingerprint is.", ""]
    qa = reports.get("qa")
    if qa and qa["status"] in ("pass", "warn"):
        L += ["## The report", "",
              f"The full report for SNAP and the dataset author is one self-contained HTML file (figures included): "
              f"`{qa['dir']}/out/report.html`. Copy it and open it in any browser.", ""]
    if release:
        L += ["## Release", "", f"Released as **{release['release']}** in `{release['dir']}` "
              f"(stage reports, report.html and a link to the coverage files).", ""]
    L += ["## Stages", "", LEGEND, "", "| Stage | Result | This run | Why |", "|---|---|---|---|"]
    for s in plan:
        r = reports.get(s["stage"])
        status = "excluded" if s["stage"] in excluded and s["status"] not in ("pass", "warn", "fail") else s["status"]
        how = "ran" if s["stage"] in ran_now else ("reused" if r else "–")
        L.append(f"| **{s['stage']}** | {label(status)} | {how} | {cell(headline(r) if r else s.get('reason', ''), 200)} |")
    L += warnings_md(reports)
    return "\n".join(L) + "\n"


def warnings_md(reports):
    """Every warning and failure across the stages, grouped by check: the ones about the results first,
    then the bookkeeping ones, each with who has it and why."""
    warns = {}
    for name, r in reports.items():
        for c in r.get("checks", []):
            if c["status"] in ("warn", "fail"):
                warns.setdefault((c["status"], c["name"]), []).append((name, c.get("detail", "")))
    science = {k: v for k, v in warns.items() if k[1] not in ROUTINE_WARNINGS}
    routine = {k: v for k, v in warns.items() if k[1] in ROUTINE_WARNINGS}
    L = []
    if science:
        L += ["", "## Warnings and failures about the results", "",
              "Each is explained in the stage's own artifact (`ngidf-<stage>`) and in the HTML report.", ""]
        for (status, name), hits in sorted(science.items(), key=lambda kv: kv[0][0] != "fail"):
            L.append(f"- {CHECK[status]} **{name}** ({', '.join(h[0] for h in hits)}): {cell(hits[0][1], 400)}")
    if routine:
        L += ["", "## Bookkeeping warnings", ""]
        for (status, name), hits in routine.items():
            why = ADOPTED_WHY if name.startswith("outputs produced") else \
                "The code these stages ran is not committed to the curation repo, so the results cannot be reproduced from a commit."
            L.append(f"- {CHECK[status]} **{name}** ({len(hits)} stages: {', '.join(h[0] for h in hits)}). {why}")
    return L

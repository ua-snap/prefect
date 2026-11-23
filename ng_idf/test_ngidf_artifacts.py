"""Tests for the NG-IDF flow's artifact text and its decision of what to run. No server, no SSH."""

import pytest

import ngidf_artifacts as art
from ngidf_pipeline import decide


def report(stage="runs", status="warn", checks=None, **kw):
    r = dict(stage=stage, title="Production model runs", explain="Runs the app for every place.", status=status,
             fingerprint="4b9f7de4cf4f3ffd" + "0" * 48, dir=f"/data/stages/{stage}/4b9f7de4cf4f3ffd",
             curation_commit="6ee0cfed837a638254664b418a7cbf770c27b448", model_label="a582a5c",
             app_commit="a582a5cf22e2142740e2839abf3f89213c27dc56", adopted=False, finalized="2026-11-19T10:57:18-09:00",
             job_ids=["750577"], metrics=dict(runs=6709, ok=6649), tables={}, summary_md="",
             components=dict(code={"ngidf/run_point.py": "a" * 64},
                             inputs=dict(reference="5ae2ff99" + "1" * 56, manifest="b" * 64)),
             checks=checks if checks is not None else [
                 dict(name="every run has a result", status="pass", detail="0 missing"),
                 dict(name="runs completed", status="warn", detail="6649 of 6709 ok"),
                 dict(name="curation code committed", status="pass", detail="the stage's code is in commit 6ee0cfe")])
    r.update(kw)
    return r


def test_artifact_key_is_valid():
    assert art.artifact_key("ngidf", "forcing_CESM_hist_LE2") == "ngidf-forcing-cesm-hist-le2"
    assert art.artifact_key("ngidf", "forcing_Daymet", "validation") == "ngidf-forcing-daymet-validation"


def test_cell_escapes_pipes_and_newlines():
    assert art.cell("max |difference|\n7.6") == "max \\|difference\\| 7.6"


def test_headline_prefers_science_warnings_over_bookkeeping():
    r = report(checks=[dict(name="outputs produced by this pipeline (not adopted)", status="warn", detail="adopted"),
                       dict(name="runs completed", status="warn", detail="6649 of 6709 ok")])
    assert art.headline(r) == "runs completed: 6649 of 6709 ok"


def test_headline_with_only_bookkeeping_warnings():
    r = report(checks=[dict(name="outputs produced by this pipeline (not adopted)", status="warn", detail="long path"),
                       dict(name="P matches the author", status="pass", detail="")])
    assert art.headline(r) == "all 1 checks on the outputs passed (outputs adopted from earlier work)"


def test_headline_failure_first():
    r = report(checks=[dict(name="runs completed", status="warn", detail="x"),
                       dict(name="every run has a result", status="fail", detail="12 missing")])
    assert art.headline(r) == "every run has a result: 12 missing"


def test_stage_markdown_has_verdict_checks_and_fingerprint():
    upstream = {"5ae2ff99" + "1" * 56: "reference"}
    md = art.stage_markdown(report(), ran_now=True,
                            slurm_jobs=[dict(job="750577", name="runs", state="COMPLETED", tasks=8)], upstream=upstream)
    assert md.startswith("# ⚠️ Production model runs")
    assert "**⚠️ Passed with warnings**: 2 passed, 1 warnings, 0 failed" in md
    assert "| ⚠️ Warning | runs completed | 6649 of 6709 ok |" in md
    assert "ran now" in md
    assert "(stage **reference**)" in md
    assert "## Slurm accounting" in md
    assert "<details>" not in md       # Prefect's markdown may not render raw HTML


def test_reused_stage_says_so():
    assert "reused: inputs and code unchanged" in art.stage_markdown(report(), ran_now=False)


def test_large_tables_go_to_table_artifacts():
    rows = [dict(cell=str(i), T_max=0.0001, W_max=0.0) for i in range(49)]
    r = report(stage="forcing_WRF_historical", tables=dict(validation=rows, small=[dict(a=1, b=2)]))
    md = art.stage_markdown(r, ran_now=False)
    assert "## Table: small" in md and "## Table: validation" not in md
    assert "`ngidf-forcing-wrf-historical-validation`" in md
    big = art.big_tables(r)
    assert list(big) == ["ngidf-forcing-wrf-historical-validation"]
    assert len(big["ngidf-forcing-wrf-historical-validation"][0]) == 49


@pytest.mark.parametrize("statuses,expected", [
    (["pass", "pass"], "pass"), (["pass", "warn"], "warn"), (["warn", "fail"], "fail"),
    (["pass", "waiting"], "waiting"), (["pass", "todo"], "waiting")])
def test_overall_status(statuses, expected):
    assert art.overall_status(statuses) == expected


def plan_of(*pairs, **extra):
    return [dict(stage=s, status=st, title=s, needs=[], **extra.get(s, {})) for s, st in pairs]


def test_decide():
    plan = plan_of(("app", "pass"), ("dhsvm", "todo"), ("reference", "waiting"), ("runs", "fail"),
                   ("coverage", "prepared"), ("qa", "prepared"), qa=dict(job_ids=["1"]))
    assert decide(plan) == dict(app="reuse", dhsvm="run", reference="wait", runs="skip-failed",
                                coverage="retry", qa="resume")
    assert decide(plan, retry_failed=True)["runs"] == "retry"


def test_decide_stop_before_excludes_the_rest_but_keeps_passed():
    plan = plan_of(("dhsvm", "todo"), ("runs", "todo"), ("coverage", "pass"), ("qa", "todo"))
    assert decide(plan, stop_before="runs") == dict(dhsvm="run", runs="exclude", coverage="reuse", qa="exclude")
    with pytest.raises(ValueError):
        decide(plan, stop_before="nope")


def test_summary_lists_science_warnings_and_report_path():
    info = dict(model=dict(MODEL_LABEL="a582a5c"))
    plan = [dict(stage="runs", status="warn"), dict(stage="qa", status="pass")]
    reports = dict(runs=report(), qa=report(stage="qa", status="pass", dir="/data/stages/qa/3f7f", checks=[]))
    md = art.summary_markdown(info, plan, reports, ran_now={"qa"})
    assert md.startswith("# ⚠️ NG-IDF pipeline, model `a582a5c`: Passed with warnings")
    assert "`/data/stages/qa/3f7f/out/report.html`" in md
    assert "**runs completed** (runs): 6649 of 6709 ok" in md
    assert "1 of 2 stages ran in this flow run" in md


def test_plan_explains_each_status_and_lists_warnings():
    info = dict(model=dict(MODEL_LABEL="a582a5c", APP_COMMIT="a582a5c"), curation_commit="da49b1a" * 6,
                curation_remote="https://github.com/ua-snap/ng-idf_curation.git", curation_branch="prefect-flow",
                curation_dirty=False, data_root="/data", slurm=dict(partition="t2small", qos="tier2"))
    adopted = dict(name="outputs produced by this pipeline (not adopted)", status="warn", detail="adopted from /x")
    plan = [dict(stage="forcing_WRF_high", status="warn", title="Forcing", needs=["places"]),
            dict(stage="runs", status="warn", title="Runs", needs=["reference"] + [f"forcing_{i}" for i in range(12)])]
    reports = dict(forcing_WRF_high=report(stage="forcing_WRF_high", checks=[adopted]), runs=report())
    md = art.plan_markdown(info, plan, dict(forcing_WRF_high="reuse", runs="reuse"), {}, reports)
    assert "**Key:** ✅ **Passed**" in md
    assert "| Stage | Now | Why | This run | Needs |" in md
    assert "all 0 checks on the outputs passed (outputs adopted from earlier work)" in md
    assert "runs completed: 6649 of 6709 ok" in md
    assert "reference, 12 forcing stages" in md
    assert "## Warnings and failures about the results" in md and "## Bookkeeping warnings" in md
    assert "made on 2026-11-17, before the pipeline existed" in md
    assert "## What a fingerprint is" in md

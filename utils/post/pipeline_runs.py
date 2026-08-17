"""Run discovery for the Prefect-native master-table builders.

The Prefect pipeline records every completed CAIRO run in
``{batch_dir}/.runs/{canonical_run_name}.path`` — a one-line file holding the
timestamped output directory.  Post-processing consumes those runs in
delivery+supply pairs, one pair per ``(scenario, stage)``, so this module turns
a pipeline YAML plus a batch name into the pairs a builder should process.

Master tables are keyed by ``{scenario}_{stage}`` (e.g. ``default_precalc``),
which replaces the legacy ``run_{delivery}+{supply}`` folder name.

Nothing here imports Prefect: the builders are plain CLIs invoked from Just.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rate_design.hp_rates.pipeline_config import (
    HP_RATES_DIR,
    PipelineConfig,
    canonical_run_name,
)

STAGES: tuple[str, ...] = ("precalc", "calibrated")
VARIANTS: tuple[str, ...] = ("delivery", "supply")

_FUSE_PREFIX = "/data.sb/"
_S3_PREFIX = "s3://data.sb/"


def pipeline_yaml_path(state: str, utility: str) -> Path:
    """Locate a utility's pipeline YAML by convention.

    ``{state}/config/scenarios/pipeline_{utility}.yaml``
    """
    path = (
        HP_RATES_DIR
        / state.lower()
        / "config"
        / "scenarios"
        / f"pipeline_{utility}.yaml"
    )
    if not path.is_file():
        raise FileNotFoundError(
            f"No pipeline YAML for utility {utility!r} at {path}. "
            f"Post-processing resolves one YAML per utility by convention."
        )
    return path


def s3_uri(path: str | Path) -> str:
    """Translate a ``/data.sb`` FUSE path to its ``s3://`` URI (idempotent).

    CAIRO run directories are recorded as FUSE paths, but the builders read
    them with polars / boto3, which want S3 URIs.
    """
    text = str(path)
    if text.startswith("s3://"):
        return text
    if text.rstrip("/") == "/data.sb":
        return _S3_PREFIX
    if text.startswith(_FUSE_PREFIX):
        return _S3_PREFIX + text[len(_FUSE_PREFIX) :]
    raise ValueError(
        f"Cannot map {text!r} to an S3 URI: expected an s3:// URI or a path "
        f"under {_FUSE_PREFIX}. Master tables are only written to S3."
    )


def batch_dir(config: PipelineConfig, batch: str) -> str:
    """The batch directory holding one utility's runs and its ``.runs/`` index."""
    return f"{config.output_base.rstrip('/')}/{config.state}/{config.utility}/{batch}"


def master_segment(scenario: str, stage: str) -> str:
    """The master-table folder name for one (scenario, stage)."""
    return f"{scenario}_{stage}"


def upgrade_for_stage(config: PipelineConfig, stage: str) -> str:
    """The ResStock upgrade a stage represents (precalc/calibrated are 1-1 with it)."""
    if stage == "precalc":
        return config.run_defaults.upgrade_precalc
    if stage == "calibrated":
        return config.run_defaults.upgrade_calibrated
    raise ValueError(f"Unknown stage {stage!r}; expected one of {list(STAGES)}")


def baseline_segment(config: PipelineConfig, yaml_path: Path | str) -> str:
    """The ``{scenario}_{stage}`` segment whose bills every other run compares to."""
    baseline = config.bill_change_baseline
    if baseline is None:
        raise ValueError(
            f"{yaml_path} has no bill_change_baseline block, so baseline bill "
            f"columns cannot be resolved. Add, for example:\n"
            f"  bill_change_baseline:\n"
            f"    scenario: default\n"
            f"    stage: precalc"
        )
    return baseline.segment


@dataclass(frozen=True, slots=True)
class RunPair:
    """The delivery + supply runs that together make one master-table segment."""

    scenario: str
    stage: str
    upgrade: str
    dir_delivery: str
    dir_supply: str

    @property
    def segment(self) -> str:
        return master_segment(self.scenario, self.stage)


def expected_segments(
    config: PipelineConfig, *, scenarios: Iterable[str] | None = None
) -> list[str]:
    """Every ``{scenario}_{stage}`` the pipeline YAML declares, in YAML order."""
    names = list(config.scenarios) if scenarios is None else list(scenarios)
    for name in names:
        config.scenario(name)  # raises KeyError on a typo
    return [master_segment(name, stage) for name in names for stage in STAGES]


def find_run_pairs(
    config: PipelineConfig,
    batch: str,
    *,
    scenarios: Iterable[str] | None = None,
) -> dict[str, RunPair]:
    """Map ``{scenario}_{stage}`` to its run pair for one utility's batch.

    A (scenario, stage) whose runs are both absent is omitted, so a partially
    run batch can still be post-processed in one command.  A pair with only one
    variant complete raises: that segment can never be built, and silently
    dropping it would hide a failed CAIRO run.
    """
    root = batch_dir(config, batch)
    names = list(config.scenarios) if scenarios is None else list(scenarios)

    pairs: dict[str, RunPair] = {}
    for name in names:
        scenario = config.scenario(name)
        for stage in STAGES:
            run_names = {
                variant: canonical_run_name(
                    config.state, config.utility, scenario.name, stage, variant
                )
                for variant in VARIANTS
            }
            delivery = read_run_dir(root, run_names["delivery"])
            supply = read_run_dir(root, run_names["supply"])
            if delivery is None and supply is None:
                continue
            if delivery is None or supply is None:
                missing = "delivery" if delivery is None else "supply"
                raise FileNotFoundError(
                    f"{config.utility} {master_segment(scenario.name, stage)}: the "
                    f"{missing} run never completed (no index file under "
                    f"{root}/.runs/), but the other variant did. Re-run the "
                    f"pipeline for this scenario before post-processing."
                )
            segment = master_segment(scenario.name, stage)
            pairs[segment] = RunPair(
                scenario=scenario.name,
                stage=stage,
                upgrade=upgrade_for_stage(config, stage),
                dir_delivery=s3_uri(delivery),
                dir_supply=s3_uri(supply),
            )
    return pairs


def build_order(segments: Iterable[str], baseline: str) -> list[str]:
    """Order segments so the baseline is built first.

    Every other segment joins the baseline's master table for its
    ``baseline_elec_*`` columns, so the baseline has to be written first.  When
    the baseline is not part of this invocation its table is expected to exist
    from an earlier run.
    """
    rest = [segment for segment in segments if segment != baseline]
    return ([baseline] if baseline in set(segments) else []) + rest


def read_run_dir(batch_dir_path: str, run_name: str) -> str | None:
    """Read one run's output directory from the run index, or None if absent.

    Prefers the local (FUSE) index file and falls back to S3, so the builders
    work on hosts where ``/data.sb`` is not mounted.
    """
    relative = f".runs/{run_name}.path"
    local = Path(batch_dir_path) / relative
    if local.is_file():
        return local.read_text().strip()
    remote = _maybe_s3_uri(batch_dir_path)
    if remote is None:
        return None
    return _s3_read_text(f"{remote.rstrip('/')}/{relative}")


def _maybe_s3_uri(path: str | Path) -> str | None:
    """``s3_uri`` for paths that live on S3, None for purely local ones."""
    try:
        return s3_uri(path)
    except ValueError:
        return None


def _s3_read_text(uri: str) -> str | None:
    """Read a small S3 text object, returning None when the key is absent."""
    import boto3
    from botocore.exceptions import ClientError

    bucket, _, key = uri[len("s3://") :].partition("/")
    try:
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise
    return body.decode().strip()

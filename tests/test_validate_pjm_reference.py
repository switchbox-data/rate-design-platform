"""Tests for the data/pjm/ curated-dataset validators.

Runs each validator CLI against the real committed CSVs (must pass) and against
tampered fixture CSVs exercising the key FAIL paths (must exit non-zero).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PJM_DIR = REPO_ROOT / "data" / "pjm"

FIVECP_VALIDATOR = PJM_DIR / "capacity" / "5cp" / "validate_fivecp_reference.py"
FIVECP_CSV = PJM_DIR / "capacity" / "5cp" / "fivecp_peaks.csv"
FIVECP_CONVERTER = PJM_DIR / "capacity" / "5cp" / "convert_5cp_md_to_csv.py"
FIVECP_SOURCES = PJM_DIR / "capacity" / "5cp" / "sources"
RPM_VALIDATOR = PJM_DIR / "capacity" / "rpm" / "validate_rpm_reference.py"
RPM_CSV = PJM_DIR / "capacity" / "rpm" / "rpm_capacity_prices.csv"
RPM_CONVERTER = PJM_DIR / "capacity" / "rpm" / "convert_rpm_md_to_csv.py"
RPM_SOURCES = PJM_DIR / "capacity" / "rpm" / "sources"
MAPPING_VALIDATOR = PJM_DIR / "zone_mapping" / "validate_pjm_zone_mapping.py"
MAPPING_GENERATOR = PJM_DIR / "zone_mapping" / "generate_zone_mapping_csv.py"


def run_validator(script: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def write_csv_with_header(df: pl.DataFrame, src: Path, dst: Path) -> None:
    """Write df to dst, preserving the # comment header of src."""
    header = [line for line in src.read_text().splitlines() if line.startswith("#")]
    body = df.write_csv()
    dst.write_text("\n".join(header) + "\n" + body)


@pytest.fixture(scope="module")
def mapping_csv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("mapping") / "pjm_utility_zone_mapping.csv"
    subprocess.run(
        [sys.executable, str(MAPPING_GENERATOR), "--path-output-csv", str(out)],
        check=True,
        capture_output=True,
    )
    return out


class TestFivecpValidator:
    def test_real_csv_passes(self) -> None:
        result = run_validator(FIVECP_VALIDATOR, ["--path-csv", str(FIVECP_CSV)])
        assert result.returncode == 0, result.stdout + result.stderr

    def _tamper_and_run(
        self, tmp_path: Path, tamper: pl.Expr | None, **kwargs
    ) -> subprocess.CompletedProcess[str]:
        df = pl.read_csv(FIVECP_CSV, comment_prefix="#", try_parse_dates=True)
        if tamper is not None:
            df = df.with_columns(tamper)
        dst = tmp_path / "fivecp_peaks.csv"
        write_csv_with_header(df, FIVECP_CSV, dst)
        return run_validator(FIVECP_VALIDATOR, ["--path-csv", str(dst)])

    def test_weekend_peak_date_fails(self, tmp_path: Path) -> None:
        # 2024-07-16 (Tue) -> 2024-07-14 (Sun)
        tamper = (
            pl.when(
                (pl.col("summer_year") == 2024)
                & (pl.col("rank") == 1)
                & (pl.col("zone") == "RTO")
            )
            .then(pl.lit("2024-07-14").str.to_date())
            .otherwise(pl.col("peak_date"))
            .alias("peak_date")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "weekend" in result.stdout

    def test_non_descending_rto_fails(self, tmp_path: Path) -> None:
        tamper = (
            pl.when(
                (pl.col("summer_year") == 2024)
                & (pl.col("rank") == 1)
                & (pl.col("zone") == "RTO")
            )
            .then(pl.lit(1.0))
            .otherwise(pl.col("mw_unrestricted"))
            .alias("mw_unrestricted")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "not strictly descending" in result.stdout

    def test_non_canonical_zone_fails(self, tmp_path: Path) -> None:
        tamper = (
            pl.when(pl.col("zone") == "DUQ")
            .then(pl.lit("DLCO"))
            .otherwise(pl.col("zone"))
            .alias("zone")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "non-canonical" in result.stdout

    def test_missing_summer_fails(self, tmp_path: Path) -> None:
        df = pl.read_csv(FIVECP_CSV, comment_prefix="#", try_parse_dates=True)
        df = df.filter(pl.col("summer_year") != 2023)
        dst = tmp_path / "fivecp_peaks.csv"
        write_csv_with_header(df, FIVECP_CSV, dst)
        result = run_validator(FIVECP_VALIDATOR, ["--path-csv", str(dst)])
        assert result.returncode == 1
        assert "missing summers" in result.stdout


class TestRpmValidator:
    def test_real_csv_passes(self) -> None:
        result = run_validator(RPM_VALIDATOR, ["--path-csv", str(RPM_CSV)])
        assert result.returncode == 0, result.stdout + result.stderr

    def _tamper_and_run(
        self, tmp_path: Path, tamper: pl.Expr
    ) -> subprocess.CompletedProcess[str]:
        df = pl.read_csv(RPM_CSV, comment_prefix="#", try_parse_dates=True)
        df = df.with_columns(tamper)
        dst = tmp_path / "rpm_capacity_prices.csv"
        write_csv_with_header(df, RPM_CSV, dst)
        return run_validator(RPM_VALIDATOR, ["--path-csv", str(dst)])

    def test_negative_price_fails(self, tmp_path: Path) -> None:
        tamper = (
            pl.when((pl.col("delivery_year") == "2025/26") & (pl.col("zone") == "AEP"))
            .then(pl.lit(-1.0))
            .otherwise(pl.col("final_zonal_capacity_price_per_mw_day"))
            .alias("final_zonal_capacity_price_per_mw_day")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "negative" in result.stdout

    def test_broken_lda_nesting_fails(self, tmp_path: Path) -> None:
        # Push the 2025/26 BGE LDA price below the RTO price.
        tamper = (
            pl.when((pl.col("delivery_year") == "2025/26") & (pl.col("lda") == "BGE"))
            .then(pl.lit(1.0))
            .otherwise(pl.col("bra_price_per_mw_day"))
            .alias("bra_price_per_mw_day")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "LDA nesting" in result.stdout or "< RTO" in result.stdout

    def test_wrong_dy_dates_fails(self, tmp_path: Path) -> None:
        tamper = (
            pl.when(pl.col("delivery_year") == "2024/25")
            .then(pl.lit("2024-01-01").str.to_date())
            .otherwise(pl.col("dy_start"))
            .alias("dy_start")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "dy_start" in result.stdout

    def test_pinned_value_mismatch_fails(self, tmp_path: Path) -> None:
        tamper = (
            pl.when((pl.col("delivery_year") == "2025/26") & (pl.col("zone") == "BGE"))
            .then(pl.lit(100.0))
            .otherwise(pl.col("bra_price_per_mw_day"))
            .alias("bra_price_per_mw_day")
        )
        result = self._tamper_and_run(tmp_path, tamper)
        assert result.returncode == 1
        assert "Cross-check" in result.stdout


class TestMappingValidator:
    def _run(self, path_csv: Path) -> subprocess.CompletedProcess[str]:
        return run_validator(
            MAPPING_VALIDATOR,
            [
                "--path-csv",
                str(path_csv),
                "--path-rpm-csv",
                str(RPM_CSV),
                "--path-fivecp-csv",
                str(FIVECP_CSV),
            ],
        )

    def test_generated_csv_passes(self, mapping_csv: Path) -> None:
        result = self._run(mapping_csv)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_missing_siblings_warns_but_passes(self, mapping_csv: Path) -> None:
        result = run_validator(
            MAPPING_VALIDATOR,
            [
                "--path-csv",
                str(mapping_csv),
                "--path-rpm-csv",
                "/nonexistent/rpm.csv",
                "--path-fivecp-csv",
                "/nonexistent/fivecp.csv",
            ],
        )
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_bad_weights_fail(self, mapping_csv: Path, tmp_path: Path) -> None:
        df = pl.read_csv(mapping_csv)
        df = df.with_columns(pl.lit(0.5).alias("capacity_weight"))
        dst = tmp_path / "mapping.csv"
        df.write_csv(dst)
        result = self._run(dst)
        assert result.returncode == 1
        assert "weights sum" in result.stdout.lower()

    def test_inconsistent_crosswalk_fails(
        self, mapping_csv: Path, tmp_path: Path
    ) -> None:
        df = pl.read_csv(mapping_csv)
        # BGE's Data Miner code is BC, not BGE.
        df = df.with_columns(
            pl.when(pl.col("utility") == "bge")
            .then(pl.lit("AEP"))
            .otherwise(pl.col("dataminer_zone"))
            .alias("dataminer_zone")
        )
        dst = tmp_path / "mapping.csv"
        df.write_csv(dst)
        result = self._run(dst)
        assert result.returncode == 1
        assert "Internal consistency" in result.stdout


class TestConvertIdempotency:
    """The committed CSVs must reproduce byte-for-byte from the intermediates.

    Guards the "GENERATED FILE - do not edit by hand" contract: a hand-edited
    CSV, or an intermediate edited without re-running `convert`, makes these fail.
    """

    def _convert(
        self, converter: Path, sources: Path, tmp_path: Path
    ) -> subprocess.CompletedProcess[str]:
        out = tmp_path / "out.csv"
        proc = subprocess.run(
            [
                sys.executable,
                str(converter),
                "--path-sources",
                str(sources),
                "--path-csv",
                str(out),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc

    def test_fivecp_csv_matches_intermediates(self, tmp_path: Path) -> None:
        proc = self._convert(FIVECP_CONVERTER, FIVECP_SOURCES, tmp_path)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        generated = (tmp_path / "out.csv").read_text()
        assert generated == FIVECP_CSV.read_text(), (
            "fivecp_peaks.csv is out of sync with sources/5cp_*.md; "
            "run `just -f data/pjm/capacity/5cp/Justfile convert`"
        )

    def test_rpm_csv_matches_intermediates(self, tmp_path: Path) -> None:
        proc = self._convert(RPM_CONVERTER, RPM_SOURCES, tmp_path)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        generated = (tmp_path / "out.csv").read_text()
        assert generated == RPM_CSV.read_text(), (
            "rpm_capacity_prices.csv is out of sync with sources/rpm_*.md; "
            "run `just -f data/pjm/capacity/rpm/Justfile convert`"
        )

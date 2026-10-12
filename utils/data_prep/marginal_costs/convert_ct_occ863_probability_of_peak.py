"""Convert OCC-863 Attachment 2 page 3 into month-hour probabilities of peak.

Page 3 holds two month-by-hour matrices: "Weekday Hourly Probability of Peak"
and "Weekend / Holiday Hourly Probability of Peak". Each cell is the share of
the distribution peak (from 2022–2025 hourly substation loads) that falls in
that month, hour, and day type. Both matrices together sum to 1.

Pages 1 and 2 are built from these matrices. Each weekday cell there is the
loss-adjusted $/kW-yr times the weekday probability, divided by that month's
weekday "No Days" count; the weekend-and-holiday matrix works the same way.
Page 3 has no day counts, so this module takes them from page 1, checks that
page 2 has the same counts, and checks that both pages are proportional to
probability / num_days.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from utils.data_prep.marginal_costs.convert_ct_occ863_month_hour_mc import (
    _MARGINAL_COST_SHEETS,
    _N_HOURS,
    _N_MONTHS,
    Voltage,
    _as_int,
    _as_number,
    _cell_text,
    month_hour_marginal_costs,
    read_sheet,
)

# The sheet name on the workbook ends in a space.
_PROBABILITY_SHEET = "OCC-863 Attachment 2 Page 3 "
_WEEKDAY_TITLE = "Weekday Hourly Probability of Peak"
_WEEKEND_TITLE = "Weekend / Holiday Hourly Probability of Peak"
_SUM_TOLERANCE = 1e-9
_RATIO_TOLERANCE = 1e-6


def month_hour_probability_of_peak(
    raw: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return the weekday and weekend-and-holiday matrices from page 3.

    Each frame has ``month`` (1–12), ``hour`` (0–23), and
    ``probability_of_peak`` (a fraction, not a percent).
    """
    rows = raw.rows()
    weekday = _melt_probability_matrix(rows, _WEEKDAY_TITLE)
    weekend = _melt_probability_matrix(rows, _WEEKEND_TITLE)
    total = float(weekday["probability_of_peak"].sum()) + float(
        weekend["probability_of_peak"].sum()
    )
    if abs(total - 1.0) > _SUM_TOLERANCE:
        raise ValueError(f"probabilities of peak sum to {total}, expected 1")
    return weekday, weekend


def _melt_probability_matrix(
    rows: list[tuple[object, ...]], title: str
) -> pl.DataFrame:
    title_idx = _find_cell(rows, 0, title)[0]
    month_idx, label_col = _find_cell(rows, title_idx, "Month >")
    hour_idx, hour_col = _find_cell(rows, month_idx, "Hour")
    month_cols = _month_columns_after(rows[month_idx], label_col)

    records: list[dict[str, int | float]] = []
    for row in rows[hour_idx + 1 :]:
        hour = _as_int(row[hour_col] if hour_col < len(row) else None)
        if hour is None:
            break
        if not 0 <= hour < _N_HOURS:
            raise ValueError(f"hour {hour} is outside 0–23 in {title!r}")
        for col, month in month_cols:
            pop = _as_number(row[col] if col < len(row) else None)
            if pop is None:
                raise ValueError(
                    f"missing {title!r} value at month {month} hour {hour}"
                )
            if not 0 <= pop <= 1:
                raise ValueError(
                    f"{title!r} value {pop} at month {month} hour {hour} is outside 0–1"
                )
            records.append({"month": month, "hour": hour, "probability_of_peak": pop})

    frame = pl.DataFrame(records).cast(
        {"month": pl.Int64, "hour": pl.Int64, "probability_of_peak": pl.Float64}
    )
    if frame.height != _N_MONTHS * _N_HOURS:
        raise ValueError(f"{title!r} has {frame.height} rows, expected 288")
    if frame.select("month", "hour").n_unique() != frame.height:
        raise ValueError(f"{title!r} repeats a month-hour")
    return frame.sort("month", "hour")


def _find_cell(
    rows: list[tuple[object, ...]], start: int, text: str
) -> tuple[int, int]:
    for idx in range(start, len(rows)):
        for col, cell in enumerate(rows[idx]):
            if _cell_text(cell) == text:
                return idx, col
    raise ValueError(f"Attachment 2 page 3 has no {text!r} cell after row {start}")


def _month_columns_after(
    row: tuple[object, ...], label_col: int
) -> list[tuple[int, int]]:
    """Month header cells to the right of "Month >".

    Columns A–G hold the long PoP table, whose numbers would otherwise be read
    as month headers.
    """
    found: list[tuple[int, int]] = []
    for col in range(label_col + 1, len(row)):
        month = _as_int(row[col])
        if month is None:
            continue
        found.append((col, month))
        if len(found) == _N_MONTHS:
            break
    months = [month for _, month in found]
    if months != list(range(1, 13)):
        raise ValueError(f"month header is {months}, expected 1–12 in order")
    return found


def attach_day_counts(
    probability: pl.DataFrame, marginal_cost: pl.DataFrame, label: str
) -> pl.DataFrame:
    """Join ``num_days`` from a page 1 or 2 matrix onto a page 3 matrix.

    Also checks that ``mc_total_per_kwh * num_days / probability_of_peak`` is
    one constant across the month-hours with a nonzero probability, and that
    the cost is zero wherever the probability is zero.
    """
    joined = probability.join(
        marginal_cost.select("month", "hour", "num_days", "mc_total_per_kwh"),
        on=["month", "hour"],
        how="inner",
        validate="1:1",
    )
    if joined.height != probability.height:
        raise ValueError(
            f"{label}: page 3 and the cost page cover different month-hours"
        )

    zero_pop_cost = joined.filter(
        (pl.col("probability_of_peak") == 0) & (pl.col("mc_total_per_kwh") != 0)
    )
    if zero_pop_cost.height:
        raise ValueError(
            f"{label}: nonzero cost where the probability of peak is zero: "
            f"{zero_pop_cost.select('month', 'hour').rows()}"
        )
    ratios = joined.filter(pl.col("probability_of_peak") > 0).select(
        pl.col("mc_total_per_kwh") * pl.col("num_days") / pl.col("probability_of_peak")
    )
    ratio_min = ratios.min().item()
    ratio_max = ratios.max().item()
    if ratio_max - ratio_min > _RATIO_TOLERANCE * abs(ratio_max):
        raise ValueError(
            f"{label}: cost is not proportional to probability_of_peak / num_days "
            f"(ratio ranges {ratio_min} to {ratio_max})"
        )
    return joined.select("month", "hour", "num_days", "probability_of_peak")


def _filename(kind: str) -> str:
    return f"month_hour_probability_of_peak_{kind}.parquet"


def write_probability_of_peak_parquets(
    path_xlsx: Path, path_output_dir: Path
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read pages 1–3 and write the weekday and weekend-and-holiday parquets."""
    if "{{" in str(path_xlsx) or "{{" in str(path_output_dir):
        raise ValueError(
            f"path looks like an uninterpolated Just variable: {path_xlsx} {path_output_dir}"
        )
    pop_weekday, pop_weekend = month_hour_probability_of_peak(
        read_sheet(path_xlsx, _PROBABILITY_SHEET)
    )

    outputs: dict[Voltage, tuple[pl.DataFrame, pl.DataFrame]] = {}
    for voltage, sheet in _MARGINAL_COST_SHEETS.items():
        _, mc_weekday, mc_weekend = month_hour_marginal_costs(
            read_sheet(path_xlsx, sheet), voltage
        )
        outputs[voltage] = (
            attach_day_counts(pop_weekday, mc_weekday, f"{voltage} weekday"),
            attach_day_counts(pop_weekend, mc_weekend, f"{voltage} weekend"),
        )

    weekday, weekend = outputs["secondary"]
    primary_weekday, primary_weekend = outputs["primary"]
    if not (weekday.equals(primary_weekday) and weekend.equals(primary_weekend)):
        raise ValueError("pages 1 and 2 give different day counts")

    path_output_dir.mkdir(parents=True, exist_ok=True)
    weekday.write_parquet(path_output_dir / _filename("weekday"))
    weekend.write_parquet(path_output_dir / _filename("weekend_holidays"))
    return weekday, weekend


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-xlsx", type=Path, required=True, help="OCC-863 Attachment 2 workbook"
    )
    parser.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for the two month_hour_probability_of_peak_* parquets",
    )
    args = parser.parse_args()
    weekday, weekend = write_probability_of_peak_parquets(
        args.path_xlsx, args.path_output_dir
    )
    for kind, frame in (("weekday", weekday), ("weekend_holidays", weekend)):
        print(
            f"Wrote {frame.height} rows to {args.path_output_dir / _filename(kind)} "
            f"(probability_of_peak sum={frame['probability_of_peak'].sum():.6f})"
        )


if __name__ == "__main__":
    main()

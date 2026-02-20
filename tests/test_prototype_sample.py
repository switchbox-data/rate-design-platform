"""Tests for optional prototype ID sampling (sample_size in scenario YAML)."""

import random

import pytest

from rate_design.ri.hp_rates.run_scenario import apply_prototype_sample


def test_apply_prototype_sample_none_returns_unchanged() -> None:
    """When sample_size is None, return prototype_ids unchanged."""
    ids = [10, 20, 30, 40, 50]
    result = apply_prototype_sample(ids, None)
    assert result is ids
    assert result == [10, 20, 30, 40, 50]


def test_apply_prototype_sample_subset() -> None:
    """When sample_size is set, return a random subset of that size."""
    ids = [10, 20, 30, 40, 50]
    rng = random.Random(42)
    result = apply_prototype_sample(ids, 2, rng=rng)
    assert len(result) == 2
    assert set(result).issubset(set(ids))
    assert len(set(result)) == 2


def test_apply_prototype_sample_subset_deterministic_with_rng() -> None:
    """With fixed RNG, same inputs yield same sample."""
    ids = [1, 2, 3, 4, 5]
    rng = random.Random(123)
    a = apply_prototype_sample(ids, 3, rng=rng)
    rng2 = random.Random(123)
    b = apply_prototype_sample(ids, 3, rng=rng2)
    assert a == b
    assert len(a) == 3


def test_apply_prototype_sample_zero_raises() -> None:
    """sample_size=0 raises ValueError (must be positive)."""
    ids = [1, 2, 3]
    with pytest.raises(ValueError, match="sample_size must be positive, got 0"):
        apply_prototype_sample(ids, 0)


def test_apply_prototype_sample_negative_raises() -> None:
    """Negative sample_size raises ValueError."""
    ids = [1, 2, 3]
    with pytest.raises(ValueError, match="sample_size must be positive, got -1"):
        apply_prototype_sample(ids, -1)


def test_apply_prototype_sample_exceeds_raises() -> None:
    """When sample_size exceeds number of prototype IDs, raise ValueError."""
    ids = [1, 2, 3, 4, 5]
    with pytest.raises(
        ValueError, match="sample_size 10 exceeds number of prototype IDs \\(5\\)"
    ):
        apply_prototype_sample(ids, 10)


def test_apply_prototype_sample_equal_len() -> None:
    """When sample_size equals len(prototype_ids), return a permutation."""
    ids = [1, 2, 3]
    rng = random.Random(0)
    result = apply_prototype_sample(ids, 3, rng=rng)
    assert len(result) == 3
    assert set(result) == set(ids)


def test_apply_prototype_sample_one() -> None:
    """sample_size=1 returns a single-element list."""
    ids = [100, 200, 300]
    rng = random.Random(99)
    result = apply_prototype_sample(ids, 1, rng=rng)
    assert len(result) == 1
    assert result[0] in ids

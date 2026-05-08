"""Tests for main.authority_score."""
import pytest


def test_zero_or_negative_yields_zero(main_module):
    assert main_module.authority_score(0) == 0
    assert main_module.authority_score(-1) == 0


def test_score_grows_with_log10_of_hosts(main_module):
    # log10(10) * 15 = 15
    assert main_module.authority_score(10) == 15
    # log10(100) * 15 = 30
    assert main_module.authority_score(100) == 30
    # log10(1000) * 15 = 45
    assert main_module.authority_score(1000) == 45


def test_score_clamped_at_100(main_module):
    # Whatever absurd input, score should not exceed 100
    assert main_module.authority_score(10**20) == 100
    assert main_module.authority_score(10**100) == 100


def test_score_monotonic(main_module):
    # Score should never decrease as num_hosts grows
    prev = -1
    for n in [1, 2, 5, 10, 50, 100, 1000, 10000, 100000, 10**8]:
        cur = main_module.authority_score(n)
        assert cur >= prev
        prev = cur

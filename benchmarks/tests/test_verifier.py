from datetime import date
from decimal import Decimal

from benchmarks.verifier import compare, lenient_equal, norm_value, strict_equal


def test_numbers_normalize_across_types():
    assert norm_value(Decimal("97690000000.0000")) == norm_value(97690000000) == norm_value(9.769e10)
    assert strict_equal([(0.123456789,)], [(0.1234568,)])  # within 1e-6 relative
    assert not strict_equal([(0.1234,)], [(0.1240,)])


def test_order_and_case_insensitive():
    assert strict_equal([("Tesla", 1), ("Boeing", 2)], [("BOEING", 2), ("tesla ", 1)])
    assert not strict_equal([("a",), ("b",)], [("b",), ("a",)], ordered=True)


def test_multiset_not_set():
    assert not strict_equal([("a",), ("a",)], [("a",)])


def test_lenient_allows_extra_columns_in_any_order():
    gold = [("Tesla", 10.0), ("NVIDIA", 20.0)]
    pred = [(7, 20.0, "nvidia"), (3, 10.0, "tesla")]
    assert not strict_equal(gold, pred)
    assert lenient_equal(gold, pred)


def test_lenient_rejects_wrong_pairing():
    gold = [("Tesla", 10.0), ("NVIDIA", 20.0)]
    pred = [("Tesla", 20.0), ("NVIDIA", 10.0)]  # same column multisets, wrong rows
    assert not lenient_equal(gold, pred)


def test_dates_and_lists():
    assert strict_equal([(date(2025, 6, 30), ["b", "a"])], [("2025-06-30", ["A", "B"])])


def test_empty_and_none():
    assert compare([], []) == {"strict": True, "lenient": True}
    assert compare([(1,)], None) == {"strict": False, "lenient": False}

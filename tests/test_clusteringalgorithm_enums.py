import pytest
from parsl.executors.adaptive_executor.executor import ClusteringAlgorithm

def test_enum_members():
    """Check that the enum has the expected clustering algorithms."""
    expected = {"FIFO", "LIFO", "GREEDY", "GREED_MIN", "GREEDY_UNLIMITED", "HEFT_GREEDY"}
    actual = {alg.name for alg in ClusteringAlgorithm}
    assert expected == actual

def test_enum_is_iterable():
    """Ensure we can iterate over the enum."""
    algs = list(ClusteringAlgorithm)
    assert len(algs) > 0
    assert all(isinstance(alg, ClusteringAlgorithm) for alg in algs)

def test_enum_string_representation():
    """Check that the enum can be converted to string and compared by name."""
    alg = ClusteringAlgorithm.FIFO
    assert str(alg) == "ClusteringAlgorithm.FIFO"
    assert alg.name == "FIFO"

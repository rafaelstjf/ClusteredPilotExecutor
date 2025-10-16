import pytest
from parsl.executors.adaptive_executor.executor import AdaptivePilotExecutor, ClusteringAlgorithm

def test_executor_accepts_enum(monkeypatch):
    """Check that the executor accepts a ClusteringAlgorithm enum parameter."""
    
    # Monkeypatch provider to avoid real submissions
    class DummyProvider:
        pass

    execu = AdaptivePilotExecutor(
        label="enum_test",
        clustering_alg=ClusteringAlgorithm.GREEDY,
        provider=DummyProvider()
    )

    assert execu.clustering_alg == ClusteringAlgorithm.GREEDY
    assert execu.label == "enum_test"

def test_executor_all_algorithms(monkeypatch):
    """Ensure executor can be initialized with all enum values."""
    class DummyProvider:
        pass

    for alg in ClusteringAlgorithm:
        execu = AdaptivePilotExecutor(
            label=f"test_{alg.name}",
            clustering_alg=alg,
            provider=DummyProvider()
        )
        assert execu.clustering_alg == alg

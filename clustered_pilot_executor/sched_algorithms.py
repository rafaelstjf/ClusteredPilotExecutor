import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeEstimate:
    """Runtime estimate generated from Parsl monitoring history.

    Args:
        estimated_runtime (float): estimated task runtime in seconds.
        number_of_samples (int): number of historical samples used.
        confidence (float): simple reliability indicator from 0.0 to 1.0.
    """

    estimated_runtime: float
    number_of_samples: int
    confidence: float


def fifo(walltime, cores, queue):
    """Schedules the tasks using the First In First Out heuristic.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    cluster = list()
    if len(queue) > cores:
        cluster = queue[:cores]
        queue = queue[cores:]
    else:
        cluster = list(queue)
        queue.clear()
    return cluster, queue


def lifo(walltime, cores, queue):
    """Schedules the tasks using the Last In First Out heuristic.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    cluster = list()
    if len(queue) > cores:
        cluster = list(reversed(queue[-cores:]))
        queue = queue[:-cores]
    else:
        cluster = list(reversed(queue))
        queue.clear()
    return cluster, queue


def estimate_runtime(db, task_name: str) -> RuntimeEstimate:
    """Estimates the runtime of a task using monitoring history.

    The estimate is the median runtime after a simple Median Absolute
    Deviation (MAD) outlier filter. If no valid sample exists, the returned
    estimate has zero samples and zero confidence.

    Args:
        db (pandas.DataFrame): dataframe containing the log of past executions.
        task_name (str): task function name to search in monitoring history.

    Returns:
        RuntimeEstimate: estimated runtime, number of samples, and confidence.
    """
    if db is None or "task_func_name" not in db or "runtime_seconds" not in db:
        return RuntimeEstimate(0.0, 0, 0.0)

    df_filtered = db[db["task_func_name"] == task_name]
    runtimes = df_filtered["runtime_seconds"].dropna()
    runtimes = runtimes[runtimes >= 0]
    if runtimes.empty:
        return RuntimeEstimate(0.0, 0, 0.0)

    median = runtimes.median()
    mad = (runtimes - median).abs().median()
    if mad > 0:
        runtimes = runtimes[(runtimes - median).abs() <= 3 * mad]
    estimated_runtime = float(runtimes.median())
    samples = int(runtimes.count())
    confidence = min(1.0, samples / 5.0)
    return RuntimeEstimate(estimated_runtime, samples, confidence)


def __calc_upward_rank(node, dag, cache):
    """Calculates the upward rank of a task in the historical DAG.

    Args:
        node (int): task identifier in the DAG.
        dag (networkx.DiGraph): DAG used in the upward rank calculation.
        cache (dict): memoization cache for previously calculated ranks.

    Returns:
        float: upward rank for the provided node.
    """
    if node in cache:
        return cache[node]

    try:
        w_node = dag.nodes[node]["runtime"]
    except KeyError:
        cache[node] = float("inf")
        return cache[node]

    successors = list(dag.successors(node))
    if len(successors) == 0:
        cache[node] = w_node
        return w_node

    max_ = 0
    for suc in successors:
        c_node_suc = 0
        rank_suc = __calc_upward_rank(suc, dag, cache)
        max_ = max(rank_suc + c_node_suc, max_)

    total = w_node + max_
    cache[node] = total
    return total


def greedy_upward_rank(dag, walltime, cores, queue):
    """Schedules tasks using a greedy upward-rank heuristic.

    This heuristic sorts ready tasks by upward rank and selects the highest
    ranked tasks for the next cluster. It is inspired by upward-rank
    prioritization, but it is not the full HEFT algorithm.

    Args:
        dag (networkx.DiGraph): DAG to be used in the upward rank calculation.
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    if dag is None:
        logger.warning("DAG history unavailable for GUR; falling back to FIFO.")
        return fifo(walltime, cores, queue)

    cache = {}
    rank = []
    for task in queue:
        task_id = task["task_id"]
        r = __calc_upward_rank(task_id, dag, cache)
        rank.append((task_id, r))

    rank.sort(key=lambda x: x[1], reverse=True)
    task_ids_to_keep = [task_id for task_id, _ in rank[:cores]]
    cluster = [
        next(task for task in queue if task["task_id"] == tid)
        for tid in task_ids_to_keep
    ]
    queue = [task for task in queue if task["task_id"] not in task_ids_to_keep]
    return cluster, queue


def _runtime_greedy(walltime, cores, db, queue, shortest: bool = False):
    """Schedules tasks by historical runtime estimates.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        db (pandas.DataFrame): dataframe containing the log of past executions.
        queue (list): list of pending tasks.
        shortest (bool): when True, sort by shortest runtime first; otherwise,
            sort by longest runtime first.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    candidates = []
    if db is None:
        logger.warning("Monitoring data not found; falling back to FIFO.")
        return fifo(walltime, cores, queue)

    for task in queue:
        task_name = task["func"].__name__
        estimate = estimate_runtime(db, task_name)
        if estimate.number_of_samples == 0:
            logger.warning(
                "No runtime samples for task %s; falling back to FIFO.", task_name
            )
            return fifo(walltime, cores, queue)
        candidates.append((task, estimate))
        logger.debug(
            "Task %s runtime estimate: %.3fs (%d samples, confidence %.2f)",
            task_name,
            estimate.estimated_runtime,
            estimate.number_of_samples,
            estimate.confidence,
        )

    queue.clear()
    candidates.sort(key=lambda x: x[1].estimated_runtime, reverse=not shortest)
    selected_time = 0.0
    cluster = []
    for candidate, estimate in candidates:
        runtime = estimate.estimated_runtime
        if max(runtime, selected_time) < walltime and len(cluster) < cores:
            cluster.append(candidate)
            selected_time = max(runtime, selected_time)
        else:
            queue.append(candidate)
    return cluster, queue


def shortest_job_first(walltime, cores, db, queue):
    """Schedules the tasks using the Shortest Job First heuristic.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        db (pandas.DataFrame): dataframe containing the log of past executions.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    return _runtime_greedy(walltime, cores, db, queue, shortest=True)


def longest_job_first(walltime, cores, db, queue):
    """Schedules the tasks using the Longest Job First heuristic.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected.
        db (pandas.DataFrame): dataframe containing the log of past executions.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    return _runtime_greedy(walltime, cores, db, queue, shortest=False)


def longest_job_first_unlimited(walltime, cores, db, queue):
    """Schedules the tasks using the Longest Job First Unlimited heuristic.

    Args:
        walltime (int): walltime in seconds.
        cores (int): number of cores/tasks to be selected. This heuristic does
            not enforce this limit when selecting tasks.
        db (pandas.DataFrame): dataframe containing the log of past executions.
        queue (list): list of pending tasks.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue.
    """
    return _runtime_greedy(walltime, float("inf"), db, queue, shortest=False)


import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def fifo(walltime, cores, queue):
    """Schedules the tasks using the First In First Out heuristic.

    Args:
        walltime (int): walltime in seconds
        cores (int): number of cores/tasks to be selected
        queue (list): list of pending tasks

    Returns:
        Tuple (list, list): returns the cluster and the modified queue
    """
    cluster  = list()
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
        walltime (int): walltime in seconds
        cores (int): number of cores/tasks to be selected
        queue (list): list of pending tasks

    Returns:
        Tuple (list, list): returns the cluster and the modified queue
    """
    cluster = list()
    if len(queue) > cores:
        cluster = list(reversed(queue[-cores:]))
        queue = queue[:-cores]
    else:
        cluster = list(reversed(queue))
        queue.clear()
    return cluster, queue

def __calc_upward_rank(node, dag, cache):
    # Memoization
    if node in cache:
        return cache[node]

    try:
        w_node = dag.nodes[node]["runtime"]  # average computation weight
    except KeyError:
        cache[node] = float("inf")
        return cache[node]

    successors = list(dag.successors(node))
    if len(successors) == 0:
        cache[node] = w_node
        return w_node

    max_ = 0
    for suc in successors:
        c_node_suc = 0  # Communication cost is considered zero
        rank_suc = __calc_upward_rank(suc, dag, cache)
        max_ = max(rank_suc + c_node_suc, max_)

    total = w_node + max_
    cache[node] = total
    return total


def hgreedy(dag, walltime, cores, queue):
    """Schedules the tasks using a greedy heuristic, sorting the tasks by their upward rank

    Args:
        dag (networkx.DIGRAPH): the dag to be used iin the upward rank calculation
        walltime (int): walltime in seconds
        cores (int): number of cores/tasks to be selected
        queue (list): list of pending tasks

    Returns:
        Tuple (list, list): returns the cluster and the modified queue
    """
    if dag is None:
        return fifo(walltime, cores, queue)

    cache = {}
    rank = []
    for task in queue:
        task_id = task["task_id"]
        r = __calc_upward_rank(task_id, dag, cache)
        rank.append((task_id, r))

    # Sort by rank value
    rank.sort(key=lambda x: x[1], reverse=True)

    # Select top tasks
    task_ids_to_keep = [task_id for task_id, _ in rank[:cores]]

    # Build cluster preserving the ranking order
    cluster = [next(task for task in queue if task["task_id"] == tid) for tid in task_ids_to_keep]

    # Remaining tasks stay in the queue
    queue = [task for task in queue if task["task_id"] not in task_ids_to_keep]

    return cluster, queue

def greedy(walltime, cores, db, queue, min_=False):
    """Schedules the tasks using a greedy heuristic, sorting the tasks by their runtime

    Args:
        walltime (int): walltime in seconds
        cores (int): number of cores/tasks to be selected
        db (pandas.DataFrame): dataframe containing the log of past executions
        queue (list): list of pending tasks
        min_ (bool, optional): Variable to inform that the sorting will be performed in ascending order. Defaults to False.

    Returns:
        Tuple (list, list): returns the cluster and the modified queue
    """
    candidates = list()
    cluster = list()
    if db is not None:
        for task in queue:
            task_name = task["func"].__name__
            df_filtered = db[db["task_func_name"] == task_name]
            if not df_filtered.empty:
                # desvio absoluto mediano (MAD)
                median = df_filtered['runtime_seconds'].median()
                mad = (df_filtered['runtime_seconds'] - median).abs().median()
                filtered_df = df_filtered[abs(df_filtered['runtime_seconds'] - median) <= 3 * mad]
                average_time = filtered_df['runtime_seconds'].mean()
            else:
                average_time = 0.0
            candidates.append((task, average_time))
            logger.debug(f"Task: {task_name} Average time (s): {average_time}")
    else:
        logger.info("Monitoring data not found! Falling back to FIFO")
        return fifo(walltime, cores, queue)
    queue.clear()
    if min_ == False:
        candidates.sort(key=lambda x: x[1], reverse=True)
        max_t = 0
        for c, t_c in candidates:
            if max(t_c, max_t) < walltime and len(cluster) + 1 < cores:
                cluster.append(c)
                max_t = max(t_c, max_t)
            else:
                queue.append(c)
    else:
        candidates.sort(key=lambda x: x[1], reverse=False)
        min_t = 0
        for c, t_c in candidates:
            if min(t_c, min_t) < walltime and len(cluster) + 1 < cores:
                cluster.append(c)
                min_t = min(t_c, min_t)
            else:
                queue.append(c)
    # if len(cluster) == 0:
    #     logger.info("Any tasks have been selected for the cluster. Falling back to FIFO")
        # return fifo(walltime, cores, queue)
    return cluster, queue

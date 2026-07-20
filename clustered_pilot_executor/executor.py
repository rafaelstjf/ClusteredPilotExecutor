import zmq
import pickle
import logging
import time
import os
import uuid
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from concurrent.futures import Future
from typing import Callable, Any, Optional, Tuple
from enum import Enum, auto

from parsl.executors.base import ParslExecutor
from parsl.serialize import pack_apply_message
from parsl.serialize.errors import SerializationError
from parsl.addresses import address_by_hostname
from parsl.providers.base import ExecutionProvider
from parsl.providers import SlurmProvider
from parsl.jobs.states import JobState
from parsl.executors.clustered_pilot_executor.dag_utils import load_tasks_from_db, load_most_similar_dag
from parsl.executors.clustered_pilot_executor.sched_algorithms import (
    fifo,
    lifo,
    shortest_job_first,
    longest_job_first,
    longest_job_first_unlimited,
    greedy_upward_rank,
)

logger = logging.getLogger(__name__)

DEFAULT_LAUNCH_CMD = "python -m parsl.executors.clustered_pilot_executor.worker tcp://{hostname}:{push_port} tcp://{hostname}:{pull_port} tcp://{hostname}:{ack_port} tcp://{hostname}:{cmd_port} {poll_time} {max_workers} {walltime}"


class ClusteringAlgorithm(Enum):
    """Supported task clustering heuristics."""

    FIFO = auto()
    LIFO = auto()
    SJF = auto()
    LJF = auto()
    LJFU = auto()
    GUR = auto()


TIME_ESTIMATE_ALGORITHMS = {
    ClusteringAlgorithm.SJF,
    ClusteringAlgorithm.LJF,
    ClusteringAlgorithm.LJFU,
    ClusteringAlgorithm.GUR,
}

ALGORITHM_MAP = {
    ClusteringAlgorithm.FIFO: fifo,
    ClusteringAlgorithm.LIFO: lifo,
    ClusteringAlgorithm.SJF: shortest_job_first,
    ClusteringAlgorithm.LJF: longest_job_first,
    ClusteringAlgorithm.LJFU: longest_job_first_unlimited,
    ClusteringAlgorithm.GUR: greedy_upward_rank,
}


class ClusteredPilotExecutor(ParslExecutor):
    radio_mode: str = "filesystem"

    def __init__(
            self,
            label: str = "ClusteredPilotExecutor",
            provider: ExecutionProvider = SlurmProvider(),
            port_range: Optional[Tuple[int, int]] = (55000, 56000),
            address: Optional[str] = address_by_hostname(),
            process_timeout: Optional[int] = 3,
            working_dir: Optional[str] = None,
            clustering_alg: Optional[ClusteringAlgorithm] = ClusteringAlgorithm.FIFO,
            allow_tasks: Optional[bool] = False,
            monitoring_db_path: str = "./runinfo/monitoring.db",
            job_status_initial_delay: float = 5.0,
            job_status_poll_interval: float = 10.0
    ):
        super().__init__()

        # General variables
        self.label = label
        self.provider = provider
        self.__validate_provider(provider)
        if working_dir is None:
            working_dir = self.label + str(uuid.uuid4())
        self.working_dir = os.path.abspath(working_dir)

        # Network variables
        self.port_range = port_range
        self.address = address

        # Parameters variables
        self.clustering_alg = clustering_alg
        self.timer = process_timeout  # Timer used to wait for new tasks
        self.process_timeout = process_timeout  # Used to reset the timer
        self.allow_tasks = allow_tasks
        self.monitoring_db_path = monitoring_db_path
        self.job_status_initial_delay = job_status_initial_delay
        self.job_status_poll_interval = job_status_poll_interval

        # --- ZMQ Context ---
        self.context = zmq.Context()

        # --- Socket to send tasks to workers ---
        self.send_task_socket = self.context.socket(zmq.PUSH)
        self.push_port = self.send_task_socket.bind_to_random_port(
            f"tcp://{self.address}",
            min_port=self.port_range[0],
            max_port=self.port_range[1],
            max_tries=100
        )

        # --- Socket to receive results from workers ---
        self.receive_task_socket = self.context.socket(zmq.PULL)
        self.pull_port = self.receive_task_socket.bind_to_random_port(
            f"tcp://{self.address}",
            min_port=self.port_range[0],
            max_port=self.port_range[1],
            max_tries=100
        )

        # --- Socket to receive the priming handshake (READY) ---
        self.ack_socket = self.context.socket(zmq.PULL)
        self.ack_port = self.ack_socket.bind_to_random_port(
            f"tcp://{self.address}",
            min_port=self.port_range[0],
            max_port=self.port_range[1],
            max_tries=100
        )

        # PUB -> workers (commands, p.ex. stop)
        self.cmd_socket = self.context.socket(zmq.PUB)
        self.cmd_port = self.cmd_socket.bind_to_random_port(
            f"tcp://{self.address}",
            min_port=self.port_range[0],
            max_port=self.port_range[1],
            max_tries=100
        )

        # Internal variables
        self.dag = None
        self.tasks = {}  # Stores the status of the tasks
        self.launched_tasks = 0  # Number of launched tasks
        self.queue = list()  # Task queue
        self.future_tasks = {}  # Stores the future objects returned when submited
        self.max_jobs = 1
        self.job_mon_interval = 30
        self.stop_send_retries = 10
        self.stop_send_interval = 0.1
        self.job_start_time = 0.0

        # Thread variables
        self.lock = Lock()  # Thread lock
        self.rcv_tasks_thread = None
        self.timer_thread = None
        self.job_monitoring_thread = None
        self.current_jobs = list()
        self.stop_event = Event()

    def __validate_provider(self, provider: ExecutionProvider) -> None:
        """Validate provider attributes required by the SLURM pilot-job workflow."""
        if not isinstance(provider, SlurmProvider):
            raise TypeError("ClusteredPilotExecutor currently requires a Parsl SlurmProvider.")
        required = ["nodes_per_block", "cores_per_node", "walltime", "submit", "status", "cancel"]
        missing = [name for name in required if not hasattr(provider, name)]
        if missing:
            raise ValueError(f"SlurmProvider is missing required attributes: {', '.join(missing)}")

    def __safe_set_future_result(self, task_id: int, result: Any) -> None:
        future = self.future_tasks.get(task_id)
        if future is None or future.cancelled() or future.done():
            logger.warning("Ignoring result for task %s because its Future is already complete or missing.", task_id)
            return
        future.set_result(result)

    def __safe_set_future_exception(self, task_id: int, exc: Exception) -> None:
        future = self.future_tasks.get(task_id)
        if future is None or future.cancelled() or future.done():
            logger.warning("Ignoring exception for task %s because its Future is already complete or missing.", task_id)
            return
        future.set_exception(exc)

    def monitor_resources(self) -> bool:
        return True

    def __send_stop_to_all(self):
        """Publica STOP redundante várias vezes e aguarda ACKs."""
        logger.debug("Publishing STOP to all workers (redundant sends)...")
        # publish redundantly to tolerate PUB/SUB startup timing
        for _ in range(self.stop_send_retries):
            if self.stop_event.is_set():
                break
            self.cmd_socket.send_multipart([b"CMD", b"STOP"])
            if self.stop_event.wait(self.stop_send_interval):
                break
        expected = self.provider.nodes_per_block
        if self.__wait_for_workers(expected, msg="STOPPED"):
            logger.debug("Workers received STOP")
        else:
            logger.debug("Failed to confirm STOP Reception")


    def __wait_for_workers(self, expected_workers: int, timeout_ms: int = 60_000, msg: str = "READY") -> bool:
        """Waits for READY messages from the workers before sending tasks."""

        logger.debug(f"Awaiting READY from {expected_workers} workers...")
        ready = 0

        poller = zmq.Poller()
        poller.register(self.ack_socket, zmq.POLLIN)

        deadline = time.time() + (timeout_ms / 1000)

        while ready < expected_workers:
            now = time.time()
            remaining = max(0, deadline - now)

            if remaining == 0:
                logger.error("Timeout waiting for workers readiness.")
                return False

            events = dict(poller.poll(remaining * 1000))

            if self.ack_socket in events:
                msg_r = self.ack_socket.recv_string()

                if msg_r == msg:
                    ready += 1
                    logger.debug(f"Worker msg ({ready}/{expected_workers})")

                else:
                    logger.warning(f"Ignoring unexpected ACK message: {msg_r}")

        logger.debug("All workers are ready. Proceeding with task dispatch.")
        return True

    def __send_tasks(self, cluster: list, send_ack=True) -> None:
        """Send tasks to the workers asynchronously."""

        if send_ack:
            # Blocking wait for the worker ack
            expected = self.provider.nodes_per_block

            if not self.__wait_for_workers(expected):
                logger.error(
                    "Could not validate worker readiness. Tasks returned to queue.")
                with self.lock:
                    for t in cluster:
                        self.queue.append(t)
                return
        for c in cluster:
            try:
                # Pack the function and arguments for execution
                task_data = pack_apply_message(
                    c["func"], c["args"], c["kwargs"])
                # Combine task ID with the packed task data
                task_metadata = pickle.dumps((c["task_id"], task_data))
                # Send the metadata to the worker
                logger.info(f"Task {c['task_id']} sent to worker")
                self.tasks[c['task_id']] = {"status": "sent"}
                self.send_task_socket.send(task_metadata)
                logger.info(f"Waiting for completition")
            except Exception as e:
                logger.error(f"Failed to send task {c['task_id']}: {e}")
                self.tasks[c['task_id']] = {"status": "error"}
                # Set the exception and let the DKF deal with the failed task
                self.__safe_set_future_exception(c['task_id'], SerializationError(c["func"]))
        if self.allow_tasks == False or self.clustering_alg in [ClusteringAlgorithm.FIFO, ClusteringAlgorithm.LIFO]:
            self.__send_stop_to_all()

    def __receive_tasks(self) -> None:
        """Receive results from workers."""
        logger.debug("Starting acknowledgment receiver")
        poller = zmq.Poller()
        poller.register(self.receive_task_socket, zmq.POLLIN)
        REC_TIMEOUT = 50_000  # Wait up to 50 seconds
        while not self.stop_event.is_set():
            try:
                event = dict(poller.poll(REC_TIMEOUT))
                if event.get(self.receive_task_socket) == zmq.POLLIN:
                    result_data = self.receive_task_socket.recv()
                    task_id, result, error = pickle.loads(result_data)
                    logger.info(f"Received result for task {task_id}")

                    if error is None:
                        with self.lock:
                            logger.info(
                                f"Task {task_id} completed successfully with result: {result}")
                            self.tasks[task_id]["status"] = "success"
                            self.__safe_set_future_result(task_id, result)
                    else:
                        with self.lock:
                            logger.error(
                                f"Task {task_id} failed with error: {error}")
                            self.tasks[task_id]["status"] = "error"
                            # Set the exception and let the DKF deal with the failed task
                            self.__safe_set_future_exception(task_id, Exception(error))
                else:
                    logger.warning("No message received within timeout.")
            except Exception as e:
                logger.error(f"Failed to process result: {e}")

    def __timer_thread(self) -> None:
        """Run the timer and process tasks when the timer reaches zero."""
        while not self.stop_event.is_set():
            while self.timer > 0 and not self.stop_event.wait(1):  # type: ignore
                self.timer -= 1  # type: ignore
            if self.stop_event.is_set():
                break
            self.__process_queue()
            self.timer = self.process_timeout

    def __monitor_jobs(self) -> None:
        """Thread to monitor the current jobs"""
        while not self.stop_event.is_set():
            with self.lock:
                has_jobs = len(self.current_jobs) > 0
            logger.debug(
                f"Monitoring jobs. Total of current jobs: {len(self.current_jobs)}")
            logger.debug(f"Processing a queue of {len(self.queue)} tasks")
            if has_jobs:
                jobs_to_remove = list()
                states = self.provider.status(self.current_jobs)
                for i in range(0, len(states)):
                    if states[i].state != JobState.RUNNING and states[i].state != JobState.PENDING:
                        jobs_to_remove.append(self.current_jobs[i])
                if len(jobs_to_remove) > 0:
                    with self.lock:
                        for j in jobs_to_remove:
                            self.current_jobs.remove(j)
            else:
                # If there are tasks marked as sent but there is no job running, this tasks probably went MIA
                with self.lock:
                    if any(t["status"] == "sent" for t in self.tasks.values()):
                        for id in self.tasks.keys():
                            if self.tasks[id]["status"] == "sent":
                                logger.error(
                                    f"Task {id} failed due to job execution time exceeded!")

                                # Set the exception and let the DKF deal with the failed task
                                self.tasks[id]["status"] = "error"
                                self.__safe_set_future_exception(id, ValueError("Task didn't receive a result"))
            if self.stop_event.wait(self.job_mon_interval):
                break

    def __process_queue(self) -> None:
        """Process tasks in the queue when the timer reaches zero and
            if the number of concurrent jobs is under the threshold
        """
        if len(self.queue) == 0:
            return
        with self.lock:  # keeping this locker to avoid refactoring when we use multiple jobs
            if (len(self.current_jobs) == self.max_jobs):
                if self.clustering_alg in [ClusteringAlgorithm.FIFO, ClusteringAlgorithm.LIFO] or self.allow_tasks == False:
                    return  # Forces FIFO to be submitted in new jobs, even though allow tasks is true
        if isinstance(self.provider, SlurmProvider):
            # Get the walltime in seconds
            datetime_obj = datetime.strptime(
                self.provider.walltime, "%H:%M:%S")
            walltime_delta = timedelta(
                hours=datetime_obj.hour, minutes=datetime_obj.minute, seconds=datetime_obj.second)
            walltime = walltime_delta.total_seconds()
            cores_old = self.provider.cores_per_node
            cores = self.provider.cores_per_node * self.provider.nodes_per_block
        else:
            walltime = int('inf')
            cores = os.cpu_count()
            cores_old = cores
        logger.debug("Trying to load monitoring database.")
        df = load_tasks_from_db(self.monitoring_db_path)
        if df is None and self.clustering_alg in TIME_ESTIMATE_ALGORITHMS:
            logger.warning("Monitoring history is unavailable or unreliable at %s; falling back to FIFO for %s.", self.monitoring_db_path, self.clustering_alg.name)
        elif self.clustering_alg in TIME_ESTIMATE_ALGORITHMS:
            logger.warning("Monitoring history is disabled; falling back to FIFO for %s.", self.clustering_alg.name)
        with self.lock:
            if len(self.current_jobs) == self.max_jobs:
                # If the executor is submitting tasks in a running job, calculate the new walltime
                REDU_FACT = 0.9
                time_diff = int(time.time() -
                                self.job_start_time)
                walltime = (walltime - time_diff)*REDU_FACT
                logger.debug(
                    f"Walltime for the running job: {walltime} seconds")
            # ------------------------------
            # creates a copy of the current queue
            queue_copy = list(self.queue)
            logger.debug(f"Processing a queue of {len(queue_copy)} tasks")

            # Limit the number of tasks to a maximum of 40
            # cores = cores - min(cores, sum([t["status"] == "sent" for t in self.tasks.values()]))
            # if cores == 0:
            #     logger.debug("Using all cores now. Skipping task processing.")
            #     return
        tasks_name = "Tasks in the queue: ["
        for i, t in enumerate(self.queue):
            tasks_name += f"{t['func'].__name__}"
            self.dag = load_most_similar_dag(
                self.dag, df, t["task_id"], t["func"].__name__)
            if i < len(self.queue)-1:
                tasks_name += ", "
        tasks_name += "]"
        logger.debug(f"{tasks_name}")
        algorithm = self.clustering_alg if isinstance(self.clustering_alg, ClusteringAlgorithm) else ClusteringAlgorithm.FIFO
        if algorithm in TIME_ESTIMATE_ALGORITHMS and df is None:
            algorithm = ClusteringAlgorithm.FIFO
        scheduler = ALGORITHM_MAP.get(algorithm, fifo)
        if algorithm == ClusteringAlgorithm.GUR:
            cluster, remaining_queue = scheduler(self.dag, walltime, cores, queue_copy)
        elif algorithm in {ClusteringAlgorithm.SJF, ClusteringAlgorithm.LJF, ClusteringAlgorithm.LJFU}:
            cluster, remaining_queue = scheduler(walltime, cores, df, queue_copy)
        else:
            cluster, remaining_queue = scheduler(walltime, cores, queue_copy)

        with self.lock:
            if len(cluster) == 0 and len(self.current_jobs) < self.max_jobs:  # type: ignore
                logger.warning(
                    "No task was added to the cluster, defaulting to FIFO!")
                cluster, remaining_queue = fifo(walltime, cores, queue_copy)
        tasks_name = "Tasks in the cluster: ["
        for i, t in enumerate(cluster):
            tasks_name += f"{t['func'].__name__}"
            if i < len(cluster)-1:
                tasks_name += ", "
        tasks_name += "]"
        logger.debug(f"Processing a cluster of {len(cluster)} tasks")
        logger.debug(f"{tasks_name}")

        processed_ids = set(t["task_id"] for t in cluster)
        with self.lock:
            self.queue = [t for t in self.queue if t["task_id"]
                          not in processed_ids]
            cur_jobs = len(self.current_jobs)
        if cur_jobs < self.max_jobs:  # type: ignore
            sub_thread = Thread(target=self.__submit_slurm_job,
                                args=(cluster, cores_old,walltime,), daemon=True)
            sub_thread.start()
        else:
            if len(cluster) > 0:
                send_thread = Thread(target=self.__send_tasks,
                                    args=(cluster, False,), daemon=True)
                send_thread.start()  # Start the task-sending thread
            elif all(t["status"] != "sent" for t in self.tasks.values()):
                #TODO add stop command
                self.__send_stop_to_all()


    def __submit_slurm_job(self, cluster: list, max_workers: int, walltime=float) -> None:
        """Submit the tasks as a job to SLURM.
        TODO: 
            - Each worker needs to have its own address
            - Support multiple jobs
        """
        # Submit the SLURM job
        with self.lock:
            logger.debug(
                "-------------------------INSIDE the submit job function-------------------------")
            if len(self.current_jobs) == self.max_jobs:
                return
            launch_cmd = DEFAULT_LAUNCH_CMD.format(hostname=self.address, pull_port=self.pull_port,
                                                   push_port=self.push_port, ack_port=self.ack_port, cmd_port=self.cmd_port, poll_time=1, max_workers=max_workers, walltime=walltime)
            logger.debug(launch_cmd)
            job_id = self.provider.submit(launch_cmd, 1)
            if not job_id:
                logger.error(f"Failed to submit SLURM job")
                raise RuntimeError(
                    "SLURM job submission returned empty job_id")
            else:
                self.current_jobs.append(job_id)

        logger.debug(f"Job {job_id} added to the current jobs list")
        status = JobState.PENDING
        # Give SLURM/provider a short, interruptible interval to register the submission.
        if self.stop_event.wait(self.job_status_initial_delay):
            logger.info("Stopping job submission monitor for %s during shutdown", job_id)
            return
        # while status == JobState.PENDING and (time.time() - start_time) < max_wait_time:
        while status == JobState.PENDING and not self.stop_event.is_set():  # Maximum waiting time removed, because the queue is really large in SDumont. Hence, it's difficult to tell how long it will take to the job start running
            status = self.provider.status([job_id])[0].state
            if status == JobState.RUNNING:
                self.job_start_time = time.time()
                logger.debug("Starting the send_tasks thread")
                send_thread = Thread(
                    target=self.__send_tasks, args=(cluster,), daemon=True)
                send_thread.start()  # Start the task-sending thread
                break
            elif status == JobState.FAILED:
                logger.error(f"Failed to submit SLURM job: {job_id}")
                with self.lock:
                    for t in cluster:
                        self.queue.append(t)
                # Exit the function instead of looping forever
                raise RuntimeError("SLURM job submission failed")
            elif status == JobState.COMPLETED:
                logger.debug(f"Job completed {job_id} - {status}")
                # If for some reason the job is completed even before running something, put back the tasks in the queue
                with self.lock:
                    for t in cluster:
                        self.queue.append(t)
                return
            else:
                logger.debug(f"Unknown status for job {job_id} - {status}")
                status = JobState.PENDING
            if self.stop_event.wait(self.job_status_poll_interval):
                logger.info("Stopping job status polling for %s during shutdown", job_id)
                return

    def start(self) -> None:
        """Starts the executor by setting up the communication channels and the timer."""
        logger.info("Executor started")
        self.launched_tasks = 0
        self.queue.clear()
        self.future_tasks.clear()
        self.tasks.clear()
        self.current_jobs.clear()
        # Start listener thread
        self.rcv_tasks_thread = Thread(target=self.__receive_tasks, daemon=False)
        self.rcv_tasks_thread.start()

        # Start timer thread
        self.timer_thread = Thread(target=self.__timer_thread, daemon=False)
        self.timer_thread.start()

        # Start job monitoring thread
        self.job_monitoring_thread = Thread(target=self.__monitor_jobs, daemon=False)
        self.job_monitoring_thread.start()

    def submit(self, func: Callable, resource_specification: dict, *args: Any, **kwargs: Any) -> Future:
        """Submit a task to the executor."""
        logger.info("Submitting task")
        # TODO: Check if with the resource especification there is need to correct the args
        # args, kwargs = self.__correct_args(args, kwargs)
        # Reset timer each time a new task is added
        with self.lock:
            task_id = self.launched_tasks
            self.launched_tasks += 1
            self.tasks[task_id] = {'status': 'queued'}
            self.queue.append({
                "task_id": task_id,
                "func": func,
                "args": args,
                "kwargs": kwargs
            })
            self.timer = self.process_timeout
        self.future_tasks[task_id] = Future()
        self.future_tasks[task_id].set_running_or_notify_cancel()
        self.future_tasks[task_id].parsl_executor_task_id = task_id

        logger.info("Clustering algorithm chosen %s", self.clustering_alg.name if isinstance(self.clustering_alg, ClusteringAlgorithm) else self.clustering_alg)
        return self.future_tasks[task_id]

    def shutdown(self) -> None:
        """Stop the executor and wait for tasks to finish."""
        logger.info("Stopping executor")
        self.stop_event.set()

        with self.lock:
            jobs_to_cancel = list(self.current_jobs)
            tasks_copy = dict(self.tasks)

        if jobs_to_cancel:
            self.provider.cancel(jobs_to_cancel)

        for task_id, task_state in tasks_copy.items():
            if task_state["status"] in ["sent", "queued"]:
                self.__safe_set_future_exception(task_id, ValueError("Task failed unexpectedly"))

        for socket in (self.send_task_socket, self.receive_task_socket, self.ack_socket, self.cmd_socket):
            socket.close(linger=0)
        self.context.term()

        # Stopping the non-daemon threads after sockets are closed to unblock pollers.
        for thread in (self.rcv_tasks_thread, self.timer_thread, self.job_monitoring_thread):
            if thread is not None:
                thread.join(timeout=10)

        logger.info(
            "All tasks have been completed or interrupted. Executor stopped.")

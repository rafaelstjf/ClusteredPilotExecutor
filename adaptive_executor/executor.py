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
from parsl.executors.adaptive_executor.dag_utils import load_df_from_db, load_most_similar_dag
from parsl.executors.adaptive_executor.sched_algorithms import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DEFAULT_LAUNCH_CMD = "python -m parsl.executors.adaptive_executor.worker tcp://{hostname}:{push_port} tcp://{hostname}:{pull_port} tcp://{hostname}:{ack_port} {timeout} {max_workers}"


class ClusteringAlgorithm(Enum):
    FIFO = auto()
    LIFO = auto()
    GREEDY = auto()
    GREED_MIN = auto()
    GREEDY_UNLIMITED = auto()
    HEFT_GREEDY = auto()


class AdaptiveExecutor(ParslExecutor):
    radio_mode: str = "filesystem"

    def __init__(
            self,
            label: str = "AdaptiveExecutor",
            provider: ExecutionProvider = SlurmProvider(),
            port_range: Optional[Tuple[int, int]] = (55000, 56000),
            address: Optional[str] = address_by_hostname(),
            process_timeout: Optional[int] = 3,
            working_dir: Optional[str] = None,
            clustering_alg: Optional[ClusteringAlgorithm] = ClusteringAlgorithm.GREEDY,
            allow_tasks: Optional[bool] = False
    ):
        super().__init__()

        # General variables
        self.label = label
        self.provider = provider
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

        # Socket variables
        self.context = zmq.Context()
        self.send_task_socket = self.context.socket(zmq.PUSH)
        self.push_port = self.send_task_socket.bind_to_random_port(
            f"tcp://{self.address}", min_port=self.port_range[0], max_port=self.port_range[1], max_tries=100)
        self.receive_task_socket = self.context.socket(zmq.PULL)
        self.pull_port = self.receive_task_socket.bind_to_random_port(
            f"tcp://{self.address}", min_port=self.port_range[0], max_port=self.port_range[1], max_tries=100)
        # Port to receive the ack when sending tasks
        self.ack_socket = self.context.socket(zmq.PULL)
        self.ack_port = self.ack_socket.bind_to_random_port(
            f"tcp://{self.address}", min_port=self.port_range[0], max_port=self.port_range[1], max_tries=100)

        # Internal variables
        self.dag = None
        self.tasks = {}  # Stores the status of the tasks
        self.launched_tasks = 0  # Number of launched tasks
        self.queue = list()  # Task queue
        self.future_tasks = {}  # Stores the future objects returned when submited
        self.max_jobs = 1  # TODO: enable more than 1 job per time
        self.job_mon_interval = 30
        self.job_start_time = 0.0

        # Thread variables
        self.lock = Lock()  # Thread lock
        self.timer_thread = None
        self.current_jobs = list()
        self.stop_event = Event()

    def monitor_resources(self) -> bool:
        return True

    def __send_tasks(self, cluster: list, send_ack=True) -> None:
        """Send tasks to the workers asynchronously."""

        # Blocking wait for the worker ack
        if send_ack:
            ACK_TIMEOUT_MS = 120_000
            try:
                logger.debug(
                    f"Waiting for the ready ACK from the worker (timeout of {ACK_TIMEOUT_MS} ms) ...")

                # Use poll to wait for the ack
                if self.ack_socket.poll(ACK_TIMEOUT_MS, zmq.POLLIN):
                    msg = self.ack_socket.recv_string()
                    if msg.strip().lower() == "ready":
                        logger.debug(
                            "ACK received. Starting the tasks dispatch.")
                    else:
                        logger.warning(
                            f"Unexpected message received in the ACK socket: {msg}")
                        return
                else:
                    logger.error(
                        "Timeout when waiting for ready ACK from worker.")
                    return
            except zmq.ZMQError as e:
                logger.error(f"Error while awaiting for ACK: {e}")
                with self.lock:
                    # put the tasks back in the queue
                    for c in cluster:
                        self.queue.append(c)
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
                self.future_tasks[c['task_id']].set_exception(
                    SerializationError(c["func"]))

    def __receive_tasks(self) -> None:
        """Receive results from workers."""
        logger.info("Starting acknowledgment receiver")
        poller = zmq.Poller()
        poller.register(self.receive_task_socket, zmq.POLLIN)
        REC_TIMEOUT = 50000  # Wait up to 5 seconds
        while not self.stop_event.is_set():
            try:
                socks = dict(poller.poll(REC_TIMEOUT))
                if self.receive_task_socket in socks and socks[self.receive_task_socket] == zmq.POLLIN:
                    result_data = self.receive_task_socket.recv()
                    task_id, result, error = pickle.loads(result_data)
                    logger.info(f"Received result for task {task_id}")

                    if error is None:
                        with self.lock:
                            logger.info(
                                f"Task {task_id} completed successfully with result: {result}")
                            self.tasks[task_id]["status"] = "success"
                            self.future_tasks[task_id].set_result(result)
                    else:
                        with self.lock:
                            logger.error(
                                f"Task {task_id} failed with error: {error}")
                            self.tasks[task_id]["status"] = "error"
                            # Set the exception and let the DKF deal with the failed task
                            self.future_tasks[task_id].set_exception(
                                Exception(error))
                else:
                    logger.warning("No message received within timeout.")
            except Exception as e:
                logger.error(f"Failed to process result: {e}")

    def __timer_thread(self) -> None:
        """Run the timer and process tasks when the timer reaches zero."""
        while not self.stop_event.is_set():
            while self.timer > 0:  # type: ignore
                time.sleep(1)
                self.timer -= 1  # type: ignore
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
                                self.future_tasks[id].set_exception(
                                    ValueError("Task didn't receive a result"))
            time.sleep(self.job_mon_interval)

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
            cores = self.provider.cores_per_node
        else:
            walltime = int('inf')
            cores = os.cpu_count()
        logger.info("Trying to load monitoring database.")
        df = load_df_from_db()
        with self.lock:
            if len(self.current_jobs) == self.max_jobs:
                # If the executor is submitting tasks in a running job, calculate the new walltime
                REDU_FACT = 0.9
                time_diff = int(time.time() -
                                self.job_start_time)
                walltime = (walltime - time_diff)*REDU_FACT
            # ------------------------------
            # creates a copy of the current queue
            queue_copy = list(self.queue)
            logger.debug(f"Processing a queue of {len(queue_copy)} tasks")
        tasks_name = "Tasks in the queue: ["
        for i, t in enumerate(self.queue):
            tasks_name += f"{t['func'].__name__}"
            self.dag = load_most_similar_dag(
                self.dag, df, t["task_id"], t["func"].__name__)
            if i < len(self.queue)-1:
                tasks_name += ", "
        tasks_name += "]"
        logger.debug(f"{tasks_name}")
        cluster = list()
        remaining_queue = list()
        if self.clustering_alg == ClusteringAlgorithm.FIFO:
            cluster, remaining_queue = fifo(walltime, cores, queue_copy)
        elif self.clustering_alg == ClusteringAlgorithm.LIFO:
            cluster, remaining_queue = lifo(walltime, cores, queue_copy)
        elif self.clustering_alg == ClusteringAlgorithm.GREEDY:
            cluster, remaining_queue = greedy(
                walltime, cores, df, queue_copy, min_=False)
        elif self.clustering_alg == ClusteringAlgorithm.GREED_MIN:
            cluster, remaining_queue = greedy(
                walltime, cores, df, queue_copy, min_=True)
        elif self.clustering_alg == ClusteringAlgorithm.GREEDY_UNLIMITED:
            cluster, remaining_queue = greedy(
                walltime, float('inf'), df, queue_copy, min_=False)
        elif self.clustering_alg == ClusteringAlgorithm.HEFT_GREEDY:
            cluster, remaining_queue = hgreedy(
                self.dag, walltime, cores, queue_copy)
        else:  # default to FIFO
            cluster, remaining_queue = fifo(walltime, cores, queue_copy)

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
            logger.info(
                "---------------------inside the lock--------------------------------")
            self.queue = [t for t in self.queue if t["task_id"]
                          not in processed_ids]
            cur_jobs = len(self.current_jobs)
        if cur_jobs < self.max_jobs:  # type: ignore
            sub_thread = Thread(target=self.__submit_slurm_job,
                                args=(cluster, cores,), daemon=True)
            sub_thread.start()
        elif len(cluster) > 0:
            send_thread = Thread(target=self.__send_tasks,
                                 args=(cluster, False,), daemon=True)
            send_thread.start()  # Start the task-sending thread

    def __submit_slurm_job(self, cluster: list, max_workers: int) -> None:
        """Submit the tasks as a job to SLURM.
        TODO: 
            - Each worker needs to have its own address
            - Support multiple jobs
        """
        # Submit the SLURM job
        with self.lock:
            logger.info(
                "-------------------------INSIDE the submit job function-------------------------")
            if len(self.current_jobs) == self.max_jobs:
                return
            launch_cmd = DEFAULT_LAUNCH_CMD.format(hostname=self.address, pull_port=self.pull_port,
                                                   push_port=self.push_port, ack_port=self.ack_port, timeout=30, max_workers=max_workers)
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
        # Sleep X seconds to await the slurm to process the job submission just to wait for slurm to process
        time.sleep(5)
        # while status == JobState.PENDING and (time.time() - start_time) < max_wait_time:
        while status == JobState.PENDING:  # Maximum waiting time removed, because the queue is really large in SDumont. Hence, it's difficult to tell how long it will take to the job start running
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
            time.sleep(10)

    def start(self) -> None:
        """Starts the executor by setting up the communication channels and the timer."""
        logger.info("Executor started")
        self.launched_tasks = 0
        self.queue.clear()
        self.future_tasks.clear()
        self.tasks.clear()
        self.current_jobs.clear()
        # Start listener thread
        rcv_tasks_thread = Thread(target=self.__receive_tasks, daemon=False)
        rcv_tasks_thread.start()

        # Start timer thread
        self.timer_thread = Thread(target=self.__timer_thread, daemon=False)
        self.timer_thread.start()

        # Start job monitoring thread
        job_monitoring = Thread(target=self.__monitor_jobs, daemon=False)
        job_monitoring.start()

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
        return self.future_tasks[task_id]

    def shutdown(self) -> None:
        """Stop the executor and wait for tasks to finish."""
        logger.info("Stopping executor")
        self.stop_event.set()

        # Stopping the non-daemon threads
        try:
            self.rcv_tasks_thread.join(timeout=10)
            self.timer_thread.join(timeout=10)
            self.job_monitoring.join(timeout=10)
        except Exception as e:
            logger.warning(f"Exception while stopping threads: {e}")

        # Wait for all tasks to finish before stopping
        # TODO: Check if this is really necessary, because now the receive task is not a daemon anymore
        tasks_to_wait = 1
        max_wait_time = 10
        start_time = time.time()
        while tasks_to_wait > 0 and (time.time() - start_time) < max_wait_time:
            time.sleep(1)
            tasks_to_wait = 0
            for t in self.tasks:
                if self.tasks[t]["status"] == "sent" or self.tasks[t]["status"] == "queued":
                    tasks_to_wait += 1
        with self.lock:
            if len(self.current_jobs) > 0:
                self.provider.cancel(self.current_jobs)
            tasks_copy = dict(self.tasks)
        for t in tasks_copy:
            if tasks_copy[t]["status"] in ["sent", "queued"]:
                with self.lock:
                    self.future_tasks[t].set_exception(
                        ValueError("Task failed unexpectedly"))
        logger.info(
            "All tasks have been completed or interrupted. Executor stopped.")

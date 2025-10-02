import zmq, pickle, logging, time, os, uuid, sqlite3
import pandas as pd
import networkx as nx
from datetime import datetime, timedelta
from parsl.executors.base import ParslExecutor
from threading import Thread, Lock
from concurrent.futures import Future
from typing import Callable, Any, Optional, Tuple
from parsl.serialize import pack_apply_message, unpack_apply_message
from parsl.serialize.errors import DeserializationError, SerializationError
from parsl.addresses import address_by_hostname
from parsl.providers.base import ExecutionProvider
from parsl.providers import SlurmProvider
from parsl.jobs.states import JobState
from parsl.executors.slurmc_executor.dag_utils import load_df_from_db, load_graph, load_most_similar_dag
from parsl.executors.slurmc_executor.sched_algorithms import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DEFAULT_LAUNCH_CMD = "python -m parsl.executors.slurmc_executor.worker tcp://{hostname}:{push_port} tcp://{hostname}:{pull_port} {timeout}"
FIFO = 0
LIFO = 1
GREEDY = 2
GREED_MIN = 3
GREEDY_UNLIMITED = 4
HEFT_GREEDY = 5

class SlurmCExecutor(ParslExecutor):
    radio_mode: str = "filesystem"
    def __init__(
            self,
            label: str = "SlurmCExecutor",
            process_timeout: Optional[int] = 3,
            max_jobs: Optional[int] = 1,
            working_dir: Optional[str] = None,
            provider: Optional[ExecutionProvider] = None,
            port_range: Tuple[int, int] | None = (55000, 56000),
            clustering_alg: Optional[int] = GREEDY
            ):
        """
            TODO:
                - The address needs to be passed as parameter in the future
        """
        super().__init__()
        # Set slurmprovider as default provider to check statuses instead of using a custom function to it
        if provider is None:
            provider = SlurmProvider()
        self.provider = provider
        self.label = label
        self.port_range = port_range
        logger.warning(f"{self.radio_mode}")
        if working_dir is None:
            working_dir = self.label + str(uuid.uuid4())
        self.working_dir = os.path.abspath(working_dir)
        self.address = address_by_hostname()
        self.clustering_alg = clustering_alg
        logger.debug(self.address)
        # Setting all the variables used on ZMQ
        self.context = zmq.Context()
        self.dag = None
        self.send_task_socket = self.context.socket(zmq.PUSH)
        self.push_port = self.send_task_socket.bind_to_random_port(f"tcp://{self.address}", min_port=self.port_range[0], max_port=self.port_range[1], max_tries=100)

        self.receive_task_socket = self.context.socket(zmq.PULL)
        self.pull_port = self.receive_task_socket.bind_to_random_port(f"tcp://{self.address}", min_port=self.port_range[0], max_port=self.port_range[1], max_tries=100)
        self.tasks = {}  # Stores the status of the tasks
        self.launched_tasks = 0  # Number of launched tasks
        self.queue = list()  # Task queue
        self.future_tasks = {}  # Stores the future objects returned when submited
        self.lock = Lock()  # Thread lock
        self.timer = process_timeout  # Timer used to wait for new tasks
        self.process_timeout = process_timeout
        self.timer_thread = None
        self.max_jobs = max_jobs
        self.current_jobs = list()
        self.running = False
        self.job_mon_interval = 30
        self.job_start_time = None
    def monitor_resources(self) -> bool:
        return True
    def __correct_args(self, args, kwargs):
        """Automatically correct the arguments to the expected format."""

        # Check if args is a tuple and contains an empty dictionary
        if isinstance(args, tuple):
            corrected_args = []
            for arg in args:
                if isinstance(arg, dict) and not arg:
                    # If the argument is an empty dictionary, we can ignore it
                    continue
                corrected_args.append(arg)
            args = tuple(corrected_args)

        # Ensure that args is a tuple (if it's not already)
        if not isinstance(args, tuple):
            args = (args,)

        # Ensure kwargs is always a dictionary (even if it's empty)
        if not isinstance(kwargs, dict):
            kwargs = {}  # Default to an empty dictionary if kwargs is not a dictionary

        return args, kwargs

    def __send_tasks(self, cluster: list) -> None:
        """Send tasks to the workers asynchronously."""
        #TODO: change it to check if the first task was received
        time.sleep(10)
        for c in cluster:
            try:
                # Pack the function and arguments for execution
                task_data = pack_apply_message(c["func"], c["args"], c["kwargs"])
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
                self.future_tasks[c['task_id']].set_exception(SerializationError(c["func"]))

    def __receive_tasks(self) -> None:
        """Receive results from workers."""
        logger.info("Starting acknowledgment receiver")
        poller = zmq.Poller()
        poller.register(self.receive_task_socket, zmq.POLLIN)
        while self.running:
            try:
                socks = dict(poller.poll(50000))  # Wait up to 5 seconds
                if self.receive_task_socket in socks and socks[self.receive_task_socket] == zmq.POLLIN:
                    result_data = self.receive_task_socket.recv()
                    task_id, result, error = pickle.loads(result_data)
                    logger.info(f"Received result for task {task_id}")

                    if error is None:
                        with self.lock:
                            logger.info(f"Task {task_id} completed successfully with result: {result}")
                            self.tasks[task_id]["status"] = "success"
                            self.future_tasks[task_id].set_result(result)
                    else:
                        with self.lock:
                            logger.error(f"Task {task_id} failed with error: {error}")
                            self.tasks[task_id]["status"] = "error"
                            self.future_tasks[task_id].set_exception(Exception(error))
                else:
                    logger.warning("No message received within timeout.")
            except Exception as e:
                logger.error(f"Failed to process result: {e}")

    def __timer_thread(self) -> None:
        """Run the timer and process tasks when the timer reaches zero."""
        while self.running:
            while self.timer > 0: # type: ignore
                time.sleep(1)
                self.timer -= 1 # type: ignore
            self.__process_queue()
            self.timer = self.process_timeout
    
    def __monitor_jobs(self) -> None:
        """Thread to monitor the current jobs"""
        while self.running:
            with self.lock:
                has_jobs = len(self.current_jobs) > 0
            logger.debug(f"monitoring jobs. Total of current jobs: {len(self.current_jobs)}")
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
                for t in self.tasks.keys():
                    if self.tasks[t]["status"] == "sent":
                        self.tasks[t]["status"] = "error"
                        self.future_tasks[t].set_exception(ValueError("Task without result"))
            time.sleep(self.job_mon_interval)

    
    def __process_queue(self) -> None:
        """Process tasks in the queue when the timer reaches zero and
            if the number of concurrent jobs is under the threshold
        """
        if len(self.queue) == 0:
            return 
        with self.lock: # keeping this locker to avoid refactoring when we use multiple jobs
            if ((len(self.current_jobs)  == self.max_jobs) and self.clustering_alg in [FIFO, LIFO]):
                return # Forces fifo to submited by level
        if isinstance(self.provider, SlurmProvider):
            datetime_obj = datetime.strptime(self.provider.walltime, "%H:%M:%S")
            walltime_delta = timedelta(hours=datetime_obj.hour, minutes=datetime_obj.minute, seconds=datetime_obj.second)
            walltime = walltime_delta.total_seconds()
            cores =self.provider.cores_per_node
        else:
            walltime = int('inf')
            cores = os.cpu_count()
        logger.info("Trying to load monitoring database.")
        df = load_df_from_db()
        with self.lock:
            if len(self.current_jobs)  == self.max_jobs:
                walltime = walltime - int(time.time() -  self.job_start_time)
            #------------------------------
            queue_copy = list(self.queue) # creates a copy of the current queue
            logger.info(f"Processing a queue of {len(queue_copy)} tasks")
        tasks_name = "Tasks in the queue: ["
        for i, t in enumerate(self.queue):
            tasks_name += f"{t['func'].__name__}"
            self.dag = load_most_similar_dag(self.dag, df, t["task_id"], t["func"].__name__)
            if i < len(self.queue)-1:
                tasks_name += ", "
        tasks_name+= "]"
        logger.info(f"{tasks_name}")
        cluster = list()
        remaining_queue = list()
        if self.clustering_alg == FIFO:
            cluster, remaining_queue = fifo(walltime, cores, queue_copy)
        elif self.clustering_alg == LIFO:
            cluster, remaining_queue = lifo(walltime, cores, queue_copy)
        elif self.clustering_alg == GREEDY:
            cluster, remaining_queue = greedy(walltime, cores, df, queue_copy, min_=False)
        elif self.clustering_alg == GREED_MIN:
            cluster, remaining_queue = greedy(walltime, cores, df, queue_copy, min_=True)
        elif self.clustering_alg == GREEDY_UNLIMITED:
            cluster, remaining_queue = greedy(walltime, float('inf'), df, queue_copy, min_=False)
        elif self.clustering_alg == HEFT_GREEDY:
            cluster, remaining_queue = hgreedy(self.dag, walltime, cores, queue_copy)
        else: #default to fifo
            cluster, remaining_queue = fifo(walltime, cores, queue_copy)

        with self.lock:
            if len(cluster) == 0 and len(self.current_jobs)  < self.max_jobs: # type: ignore
                logger.warning("No task was added to the cluster, defaulting to FIFO!")
                cluster, remaining_queue = fifo(walltime, cores, queue_copy)
        tasks_name = "Tasks in the cluster: ["
        for i, t in enumerate(cluster):
            tasks_name += f"{t['func'].__name__}"
            if i < len(cluster)-1:
                tasks_name += ", "
        tasks_name+= "]"
        logger.info(f"Processing a cluster of {len(cluster)} tasks")
        logger.info(f"{tasks_name}")

        processed_ids = set(t["task_id"] for t in cluster)
        with self.lock:
            logger.info("---------------------inside the lock--------------------------------")
            self.queue = [t for t in self.queue if t["task_id"] not in processed_ids]
            cur_jobs = len(self.current_jobs)
        if cur_jobs  < self.max_jobs: # type: ignore
            sub_thread = Thread(target=self.__submit_slurm_job, args=(cluster,), daemon=True)
            sub_thread.start()
        elif len(cluster) > 0:
            send_thread = Thread(target=self.__send_tasks, args=(cluster,), daemon=True)
            send_thread.start()  # Start the task-sending thread
        

    def __submit_slurm_job(self, cluster: list) -> None:
        """Submit the tasks as a job to SLURM.
        TODO: 
            - Each worker needs to have its own address
            - Support multiple jobs
        """
        # Submit the SLURM job
        with self.lock:
            logger.info("-------------------------INSIDE the submit job function-------------------------")
            if len(self.current_jobs) == self.max_jobs:
                return
            launch_cmd = DEFAULT_LAUNCH_CMD.format(hostname = self.address, pull_port = self.pull_port, push_port = self.push_port, timeout = 20)
            logger.debug(launch_cmd)
            job_id = self.provider.submit(launch_cmd, 1)
            if not job_id:
                logger.error(f"Failed to submit SLURM job")
                raise RuntimeError("SLURM job submission returned empty job_id")
            else:
                self.current_jobs.append(job_id)
        # Moved outside the locker to not hold the system for too long
        logger.debug(f"Job {job_id} added to the current jobs list")
        status = JobState.PENDING
        max_wait_time = 60  # Max wait 60 seconds for job to start
        start_time = time.time()
        time.sleep(5) # Sleep X seconds to await the slurm to process the job submission
        #while status == JobState.PENDING and (time.time() - start_time) < max_wait_time:
        while status == JobState.PENDING: # Maximum waiting time removed, because the queue is really large in SDumont. Hence, it's difficult to tell how long it will take to the job start running
            status = self.provider.status([job_id])[0].state
            if status == JobState.RUNNING:
                self.job_start_time = time.time()
                logger.debug("Starting the send_tasks thread")
                send_thread = Thread(target=self.__send_tasks, args=(cluster,), daemon=True)
                send_thread.start()  # Start the task-sending thread
                break
            elif status == JobState.FAILED:
                logger.error(f"Failed to submit SLURM job: {job_id}")
                raise RuntimeError("SLURM job submission failed")  # Exit the function instead of looping forever
            elif status == JobState.COMPLETED:
                logger.debug(f"Job completed {job_id} - {status}")
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
        self.running = True
        # Start listener thread
        ack_thread = Thread(target=self.__receive_tasks, daemon=True)
        ack_thread.start()

        # Start timer thread
        self.timer_thread = Thread(target=self.__timer_thread, daemon=True)
        self.timer_thread.start()

        # Start job monitoring thread
        job_monitoring = Thread(target=self.__monitor_jobs, daemon=True)
        job_monitoring.start()

    def submit(self, func: Callable, *args: Any, **kwargs: Any) -> Future:
        """Submit a task to the executor."""
        logger.info("Submitting task")
        task_id = self.launched_tasks
        self.launched_tasks += 1
        self.tasks[task_id] = {'status': 'queued'}
        args, kwargs = self.__correct_args(args, kwargs)
        # Reset timer each time a new task is added
        with self.lock:
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
        self.running = False
        # Wait for all tasks to finish before stopping
        tasks_to_wait = 0
        for t in self.tasks:
            if self.tasks[t]["status"] == "sent" or self.tasks[t]["status"] == "queued":
                tasks_to_wait += 1
        max_wait_time = 10  # Max wait 60 seconds for job to start
        start_time = time.time()
        while tasks_to_wait > 0 and (time.time() - start_time) < max_wait_time:
            time.sleep(1)
            tasks_to_wait = 0
            for t in self.tasks:
                if self.tasks[t]["status"] == "sent" or self.tasks[t]["status"] == "queued":
                    tasks_to_wait += 1
        with self.lock:
            tasks_copy = dict(self.tasks)
            future_copy = dict(self.future_tasks)
        for t in tasks_copy:
            if tasks_copy[t]["status"] in ["sent", "queued"]:
                future_copy[t].set_exception(ValueError("Task failed unexpectedly"))
        with self.lock:
            if len(self.current_jobs) > 0:
                self.provider.cancel(self.current_jobs)
        logger.info("All tasks have been completed. Executor stopped.")
    
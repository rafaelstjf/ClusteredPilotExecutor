import datetime
import logging
import pickle
import queue
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import zmq
from parsl.serialize import unpack_apply_message

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
context = zmq.Context()
receiver = None
sender = None

class ResourceScheduler:
    """
    Controls the number of CPU cores available to execute the tasks.
    This considers that Parsl can submit multithreaded tasks with cores in the resource specification.
    """
    def __init__(self, total_cpus):
        if total_cpus <= 0:
            raise ValueError("Number of CPUs invalid!")
        self.total_cpus = total_cpus
        self.available_cpus = total_cpus
        self._lock = threading.Lock()

    def can_run(self, cores):
        with self._lock:
            return cores > 0 and cores <= self.available_cpus

    def acquire(self, cores):
        with self._lock:
            if cores <= 0 or cores > self.available_cpus:
                raise RuntimeError("Not enough CPU resources to run requested task")
            self.available_cpus -= cores

    def release(self, cores):
        with self._lock:
            self.available_cpus += cores
            self.available_cpus = min(self.total_cpus, self.available_cpus)




def process_task(task_id, func, args, kwargs):
    """Process the received task."""
    try:
        logger.info(f"Processing task {task_id}")
        result = func(*args, **kwargs)
        logger.info(f"Task {task_id} completed successfully")
        return task_id, result, None
    except Exception as e:
        logger.info(f"Task {task_id} failed with error: {e}")
        return task_id, None, str(e)


def send_callback(future, sender):
    task_id, result, error = future.result()
    result_data = pickle.dumps((task_id, result, error))
    sender.send(result_data)
    logger.info(f"Task {task_id} result was sent back to the executor")


def send_error_callback(future, sender, error):
    task_id = future.result()[0]
    result_data = pickle.dumps((task_id, None, error))
    sender.send(result_data)
    logger.info(f"Task {task_id} error was sent back to the executor")



def worker_task(
    receiver, sender, commands, ack_sender, poll_time, max_workers, walltime
):
    """Worker function to process tasks received over ZeroMQ with auto-termination."""
    logger.info("Worker started and waiting for tasks")
    accepting_tasks = True
    pending_tasks = deque()
    callback_queue = queue.Queue()
    scheduler = ResourceScheduler(max_workers) #number of CPUs == number of workers
    running_tasks = {}
    poller = zmq.Poller()

    max_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=walltime
    )

    poller.register(receiver, zmq.POLLIN)
    poller.register(commands, zmq.POLLIN)
    running = True
    stop_requested = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while running:
                # If the walltime was over
                if (
                    accepting_tasks
                    and datetime.datetime.now(datetime.timezone.utc)
                    >= max_time - datetime.timedelta(seconds=60)
                ):
                    accepting_tasks = False
                    try:
                        poller.unregister(receiver)
                    except zmq.ZMQError:
                        pass

                sockets = dict(poller.poll(poll_time * 1000))

                if commands in sockets and sockets[commands] == zmq.POLLIN:
                    reading_commands = True
                    while reading_commands:
                        try:
                            topic, payload = commands.recv_multipart(zmq.NOBLOCK)
                        except zmq.Again:
                            reading_commands = False
                            continue
                        if topic == b"CMD" and payload == b"STOP":
                            logger.info("Received STOP command")
                            stop_requested = True
                            accepting_tasks = False
                            try:
                                poller.unregister(receiver)
                            except zmq.ZMQError:
                                pass
                        else:
                            logger.warning("Invalid message received!")

                if accepting_tasks and receiver in sockets and sockets[receiver] == zmq.POLLIN:
                    reading_tasks = True
                    while reading_tasks:
                        try:
                            task_metadata = receiver.recv(zmq.NOBLOCK)
                        except zmq.Again:
                            reading_tasks = False
                            continue
                        try:
                            message = pickle.loads(task_metadata)
                            if len(message) < 2:
                                raise ValueError("Invalid task message!")
                            task_id = message[0]
                            task_data = message[1]
                            cores = int(message[2]) if len(message) == 3 else 1
                            if cores <= 0:
                                raise ValueError("cores must be greater than zero")
                            func, args, kwargs = unpack_apply_message(task_data)
                            pending_tasks.append(
                                (task_id, func, args, kwargs, cores)
                            )
                        except Exception:
                            logger.exception("Failed to decode task")

                while pending_tasks:
                    task_id, func, args, kwargs, cores = pending_tasks[0]
                    if cores > max_workers:
                        pending_tasks.popleft()
                        future = executor.submit(
                            lambda tid, msg: (tid, None, msg),
                            task_id,
                            (
                                f"Task requires {cores} CPUs but worker has only "
                                f"{max_workers}"
                            ),
                        )
                        running_tasks[future] = task_id
                        future.add_done_callback(callback_queue.put)
                        continue
                    if not scheduler.can_run(cores):
                        break
                    pending_tasks.popleft()
                    scheduler.acquire(cores)
                    future = executor.submit(process_task, task_id, func, args, kwargs)
                    running_tasks[future] = task_id

                    def task_done(
                        completed_future,
                        cores=cores,
                    ):
                        scheduler.release(cores)
                        callback_queue.put(completed_future)

                    future.add_done_callback(task_done)

                draining_callbacks = True
                while draining_callbacks:
                    try:
                        future = callback_queue.get_nowait()
                    except queue.Empty:
                        draining_callbacks = False
                        continue
                    try:
                        send_callback(future, sender)
                    except Exception as exc:
                        error = f"Failed to send task callback: {exc}"
                        logger.exception("Failed to send callback; sending error callback")
                        try:
                            send_error_callback(future, sender, error)
                        except Exception:
                            logger.exception("Failed to send error callback")
                    finally:
                        running_tasks.pop(future, None)

                if (
                    not accepting_tasks
                    and not pending_tasks
                    and not running_tasks
                    and callback_queue.empty()
                ):
                    logger.info("All tasks drained! Worker can terminate.")
                    running = False

            logger.info("Worker entering final callback drainage.")
            draining_final_callbacks = True
            while draining_final_callbacks:
                try:
                    future = callback_queue.get_nowait()
                except queue.Empty:
                    draining_final_callbacks = False
                    continue
                try:
                    send_callback(future, sender)
                except Exception as exc:
                    error = f"Failed to send task callback: {exc}"
                    logger.exception("Failed to send final callback; sending error callback")
                    try:
                        send_error_callback(future, sender, error)
                    except Exception:
                        logger.exception("Failed to send final error callback")
                finally:
                    running_tasks.pop(future, None)

            if stop_requested:
                ack_sender.send_string("STOPPED")
            logger.info("Worker completed successfully.")

        except Exception:
            logger.exception(
                "Worker encountered an unexpected error."
            )

        finally:
            # Closing all connections
            if receiver:
                try:
                    poller.unregister(receiver)
                except zmq.ZMQError:
                    pass
                try:
                    receiver.close(linger=0)
                except zmq.ZMQError:
                    pass

            if commands:
                try:
                    poller.unregister(commands)
                except zmq.ZMQError:
                    pass
                try:
                    commands.close(linger=0)
                except zmq.ZMQError:
                    pass

            try:
                sender.close(linger=-1)
            except zmq.ZMQError:
                pass

            logger.info("Worker has shut down.")


def main() -> None:
    import sys

    if len(sys.argv) != 8:
        logger.error(
            "Usage: python -m parsl.executors.clustered_pilot_executor.worker <receiver_address> <sender_address> <ack_address> <commands_address> <poll_time> <max_workers> <walltime>"
        )
        sys.exit(1)

    receiver_address = sys.argv[1]
    sender_address = sys.argv[2]
    ack_address = sys.argv[3]
    commands_address = sys.argv[4]
    poll_time = int(sys.argv[5])
    max_workers = int(sys.argv[6])
    walltime = float(sys.argv[7])

    context = zmq.Context()

    ack_sender = context.socket(zmq.PUSH)
    ack_sender.connect(ack_address)
    logger.info(f"Sending READY to {ack_address}")
    ack_sender.send_string("READY")

    receiver = context.socket(zmq.PULL)
    receiver.connect(receiver_address)

    sender = context.socket(zmq.PUSH)
    sender.connect(sender_address)

    commands = context.socket(zmq.SUB)
    commands.connect(commands_address)
    commands.setsockopt(zmq.SUBSCRIBE, b"CMD")

    try:
        worker_task(
            receiver, sender, commands, ack_sender, poll_time, max_workers, walltime
        )
    finally:
        ack_sender.close(linger=0)
        context.term()


if __name__ == "__main__":
    main()

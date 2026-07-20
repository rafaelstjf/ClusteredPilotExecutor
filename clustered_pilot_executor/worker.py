import zmq
import pickle
import logging
import time
import datetime
from parsl.serialize import pack_apply_message, unpack_apply_message
import queue
from concurrent.futures import ThreadPoolExecutor, wait

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
context = zmq.Context()
receiver = None
sender = None
callback_queue = queue.Queue()


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


def enqueue_callback(fut):
    callback_queue.put(fut)


def worker_task(
    receiver, sender, commands, ack_sender, poll_time, max_workers, walltime
):
    """Worker function to process tasks received over ZeroMQ with auto-termination."""
    logger.info("Worker started and waiting for tasks")
    futures = []
    poller = zmq.Poller()
    running = True
    max_time = datetime.datetime.now() + datetime.timedelta(seconds=walltime)

    poller.register(receiver, zmq.POLLIN)
    poller.register(commands, zmq.POLLIN)

    stop = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while running:
                sockets = dict(poller.poll(poll_time * 1000))

                # Receiving tasks
                if receiver and receiver in sockets and sockets[receiver] == zmq.POLLIN:
                    while running:
                        try:
                            task_metadata = receiver.recv(zmq.NOBLOCK)
                        except zmq.Again:
                            break

                        task_id, task_data = pickle.loads(task_metadata)
                        logger.info(f"Task id {task_id} received in the worker")
                        func, args, kwargs = unpack_apply_message(task_data)

                        f = executor.submit(process_task, task_id, func, args, kwargs)
                        f.add_done_callback(enqueue_callback)
                        futures.append(f)

                # if the message is a command
                if commands in sockets and sockets[commands] == zmq.POLLIN:
                    try:
                        msg = commands.recv_multipart()
                        if len(msg) == 2:
                            topic, payload = msg
                            if topic == b"CMD" and payload == b"STOP":
                                logger.info("Received STOP command.")
                                stop = True
                                ack_sender.send_string("STOPPED")
                        else:
                            logger.warning("Invalid message received!")
                    except zmq.Again:
                        pass
                    except Exception:
                        logger.exception("Failed to receive/process command")

                # Sending the results
                while True:
                    try:
                        fut = callback_queue.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        send_callback(fut, sender)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")

                futures = [f for f in futures if not f.done()]

                # verifying the remaining time

                if (
                    not stop
                    and datetime.datetime.now()
                    >= max_time - datetime.timedelta(seconds=60)
                ):
                    logger.info("Time threshold reached — stop receiving new tasks.")
                    stop = True
                    if receiver:
                        try:
                            poller.unregister(receiver)
                        except Exception:
                            pass
                        receiver = None

                # Getting out of the main loop
                if stop and not futures and callback_queue.empty():
                    running = False

            # Final drainage, waiting for all futures
            try:
                wait(futures)
            except Exception:
                pass

            while True:
                try:
                    fut = callback_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    send_callback(fut, sender)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

        except Exception as e:
            logger.error(f"Worker encountered an error: {e}")

        finally:
            # Closing all connections
            if receiver:
                try:
                    poller.unregister(receiver)
                except Exception:
                    pass
                try:
                    receiver.close(linger=0)
                except Exception:
                    pass

            if commands:
                try:
                    poller.unregister(commands)
                except Exception:
                    pass
                try:
                    commands.close(linger=0)
                except Exception:
                    pass

            try:
                sender.close(linger=-1)
            except Exception:
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

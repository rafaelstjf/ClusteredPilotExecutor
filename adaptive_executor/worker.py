import zmq
import pickle
import logging
import time
from parsl.serialize import pack_apply_message, unpack_apply_message
from concurrent.futures import ThreadPoolExecutor, wait
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
context = zmq.Context()
receiver = None
sender = None
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

def worker_task(receiver, sender, timeout, max_workers):
    """Worker function to process tasks received over ZeroMQ with auto-termination."""
    logger.info("Worker started and waiting for tasks")
    futures = []
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)  # Listen for incoming message
    last_task_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while True:
                events = poller.poll(timeout*1000) # Timeout is passed as parameter as seconds
                if events:
                    # Receive and unpack the task
                    task_metadata = receiver.recv()
                    task_id, task_data = pickle.loads(task_metadata)
                    logger.info(f"Task id {task_id} received in the worker")
                    func, args, kwargs = unpack_apply_message(task_data)

                    # Process the task
                    f = executor.submit(process_task, task_id, func, args, kwargs)
                    f.add_done_callback(lambda fut: send_callback(fut, sender))
                    futures.append(f)
                    last_task_time = time.time()
                still_running = any(not fut.done() for fut in futures)
                if not events and not still_running and (time.time() - last_task_time) > timeout:
                    logger.info("No tasks received within timeout. Worker shutting down.")
                    break  # Exit if no message arrives within the timeout

        except Exception as e:
            logger.error(f"Worker encountered an error: {e}")

        finally:
            wait(futures)  # Aguarda todas as tarefas terminarem
            if receiver in poller.sockets:
                poller.unregister(receiver)
            poller.unregister(receiver)  # Unregister before closing
            receiver.close()
            sender.close()
            context.term()
            logger.info("Worker has shut down.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 6:
        print("Usage: python worker.py <input_address> <output_address> <ack_address> <timeout> <max_workers>")
        sys.exit(1)

    input_address = sys.argv[1]     # e.g., tcp://<executor>:5555
    output_address = sys.argv[2]    # e.g., tcp://<executor>:5556
    ack_address = sys.argv[3]       # e.g., tcp://<executor>:5560
    timeout = int(sys.argv[4])
    max_workers = int(sys.argv[5])

    # Socket para receber tarefas
    receiver = context.socket(zmq.PULL)
    receiver.connect(input_address)

    # Socket para enviar resultados
    sender = context.socket(zmq.PUSH)
    sender.connect(output_address)

    # Novo socket para enviar ACK de prontidão
    ack_sender = context.socket(zmq.PUSH)
    ack_sender.connect(ack_address)
    logger.info(f"Enviando ACK de prontidão para {ack_address}")
    ack_sender.send_string("ready")
    ack_sender.close()

    # Inicia o loop principal do worker
    worker_task(receiver, sender, timeout, max_workers)
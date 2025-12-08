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
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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
    
def worker_task(receiver, sender, commands, poll_time, max_workers, walltime):
    """Worker function to process tasks received over ZeroMQ with auto-termination."""
    """
    worker sempre rodando até receber o sinal de parada
    """
    logger.info("Worker started and waiting for tasks")
    futures = []
    poller = zmq.Poller()
    running = True
    max_time = datetime.datetime.now() + datetime.timedelta(seconds=walltime)
    poller.register(receiver, zmq.POLLIN)  # Listen for incoming message
    poller.register(commands, zmq.POLLIN)
    stop = False
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while running:
                sockets = dict(poller.poll(poll_time*1000)) # get all registered sockets in the event

                # Process the tasks
                if receiver in sockets and sockets[receiver] == zmq.POLLIN:
                    while running:
                        try:
                            task_metadata = receiver.recv(zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        # Receive andd unpack the task
                        task_id, task_data = pickle.loads(task_metadata)
                        logger.info(f"Task id {task_id} received in the worker")
                        func, args, kwargs = unpack_apply_message(task_data)
                        # Process the task
                        f = executor.submit(process_task, task_id, func, args, kwargs)
                        f.add_done_callback(enqueue_callback)
                        futures.append(f)
                        last_task_time = time.time()

                # Check if it received the stop command
                if commands in sockets and sockets[commands] == zmq.POLLIN:
                    try:
                        msg = commands.recv_json(flags=zmq.NOBLOCK)
                        if msg.get("cmd") == "STOP":
                            logger.info("Received STOP command.")
                            stop = True
                    except zmq.Again:
                        pass
                    except Exception:
                        logger.exception("Failed to receive/process task")
                
                # Send the available results, emptying the queue
                while running:
                    try:
                        fut = callback_queue.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        send_callback(fut, sender)   # envia resultado
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                futures = [f for f in futures if not f.done()]
                # confere se não tem nenhum future para processar ouse está atingindo o tempo máximo
                if stop or (max_time - datetime.datetime.now()).total_seconds() < 60:
                    running = False

        except Exception as e:
            logger.error(f"Worker encountered an error: {e}")
            
        finally:
            #instantaneamente sai do receiver e do commands
            try:
                poller.unregister(receiver)
                poller.unregister(commands)
            except Exception:
                pass
            try:
                commands.close(linger=0)
            except Exception:
                pass
            try:
                receiver.close(linger=0)
            except Exception:
                pass
            # Espera a fila de trabalhos pendentes terminar
            try:
                wait(futures)
                while True:
                    try:
                        fut = callback_queue.get_nowait()
                    except queue.Empty:
                        break

                    try:
                        send_callback(fut, sender)   # envia resultado
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
            except Exception:
                pass
            try:
                sender.close(linger=0)
            except Exception:
                pass

            logger.info("Worker has shut down.")



if __name__ == "__main__":
    import sys
    if len(sys.argv) != 8:
        print("Usage: python worker.py <receiver_addres> <sender_address> <ack_address> <commands_address> <poll_time> <max_workers> <walltime>")
        sys.exit(1)

    receiver_addres = sys.argv[1]
    sender_address = sys.argv[2]
    ack_address = sys.argv[3]
    commands_address = sys.argv[4]
    poll_time = int(sys.argv[5])
    max_workers = int(sys.argv[6])
    walltime = float(sys.argv[7])

    context = zmq.Context()


    # Socket to send readiness ACK (PUSH)
    ack_sender = context.socket(zmq.PUSH)
    ack_sender.connect(ack_address)

    # --- Priming handshake ---
    logger.info(f"Enviando READY para {ack_address}")
    ack_sender.send_string("READY")



    # Socket to receive tasks (PULL)
    receiver = context.socket(zmq.PULL)
    receiver.connect(receiver_addres)

    # Socket to send results (PUSH)
    sender = context.socket(zmq.PUSH)
    sender.connect(sender_address)

    # SUB para comandos
    commands = context.socket(zmq.SUB)
    commands.connect(commands_address)
    commands.setsockopt_string(zmq.SUBSCRIBE, "")


    # --- Worker loop ---
    worker_task(receiver, sender, commands, poll_time, max_workers, walltime)

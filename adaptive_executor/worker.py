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
    """Worker function to process tasks received over ZeroMQ with guaranteed termination."""
    logger.info("Worker started and waiting for tasks")

    futures = []
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)

    last_task_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while True:
                events = poller.poll(timeout * 1000)

                real_message_received = False

                if events:
                    # Tentamos receber SEM bloquear para diferenciar
                    # eventos falsos do ZMQ de mensagens reais.
                    try:
                        task_metadata = receiver.recv(zmq.NOBLOCK)
                        real_message_received = True
                    except zmq.Again:
                        # Evento falso-positivo: não há mensagem real
                        pass

                if real_message_received:
                    # Mensagem real recebida → processar corretament
                    try:
                        task_id, task_data = pickle.loads(task_metadata)
                        func, args, kwargs = unpack_apply_message(task_data)
                    except Exception as e:
                        logger.error(f"Erro ao decodificar tarefa: {e}")
                        # future artificial para notificar executor
                        f = executor.submit(lambda: (task_id, None, str(e)))
                        f.add_done_callback(lambda fut: send_callback(fut, sender))
                        futures.append(f)
                        last_task_time = time.time()
                        continue

                    # Enviar para o pool
                    f = executor.submit(process_task, task_id, func, args, kwargs)
                    f.add_done_callback(lambda fut: send_callback(fut, sender))
                    futures.append(f)
                    last_task_time = time.time()

                # Limpar futures terminadas
                futures = [f for f in futures if not f.done()]

                # Condição clara de saída:
                # 1. Nenhuma mensagem real recebida
                # 2. Nenhuma future rodando
                # 3. Tempo ocioso ultrapassou timeout
                if not real_message_received \
                   and not futures \
                   and (time.time() - last_task_time) > timeout:
                    logger.info(
                        f"No activity for {timeout}s. Worker shutting down gracefully."
                    )
                    break

        except Exception:
            logger.exception("Worker encountered an error")

        finally:
            try:
                wait(futures, timeout=5)
            except Exception:
                pass

            try:
                poller.unregister(receiver)
            except Exception:
                pass

            try:
                receiver.close(linger=0)
            except Exception:
                pass

            try:
                sender.close(linger=0)
            except Exception:
                pass

            logger.info("Worker has shut down.")



if __name__ == "__main__":
    import sys

    if len(sys.argv) != 6:
        print("Usage: python worker.py <input_address> <output_address> <ack_address> <timeout> <max_workers>")
        sys.exit(1)

    input_address = sys.argv[1]
    output_address = sys.argv[2]
    ack_address = sys.argv[3]
    timeout = int(sys.argv[4])
    max_workers = int(sys.argv[5])

    context = zmq.Context()


    # Socket to send readiness ACK (PUSH)
    ack_sender = context.socket(zmq.PUSH)
    ack_sender.connect(ack_address)

    # --- Priming handshake ---
    logger.info(f"Enviando READY para {ack_address}")
    ack_sender.send_string("READY")



    # Socket to receive tasks (PULL)
    receiver = context.socket(zmq.PULL)
    receiver.connect(input_address)

    # Socket to send results (PUSH)
    sender = context.socket(zmq.PUSH)
    sender.connect(output_address)

    # Obs.: você pode fechar o socket após enviar se quiser:
    # ack_sender.close()

    # --- Worker loop ---
    worker_task(receiver, sender, timeout, max_workers)

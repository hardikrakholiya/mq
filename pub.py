import time
import zmq
from gateway_api import pub, sub
from threading import Thread


msg = {
    "message": "Hello",
    "timestamp": 0,
    "topic": "Queue-1"
}


def main():
    """main method"""

    # Prepare our context and publisher
    context = zmq.Context()
    publisher = context.socket(zmq.REQ)
    publisher.connect("tcp://localhost:5565")
    # Write two messages, each with an envelope and content
    msg["timestamp"] = time.time()
    # sendgtwy = gateway_api.gateway()
    ack = pub(publisher, msg)
    print ack

    # We never get here but clean up anyhow
    publisher.close()
    context.term()


if __name__ == "__main__":
    Thread(target=main).start()
    Thread(target=main).start()
    Thread(target=main).start()
    Thread(target=main).start()
    Thread(target=main).start()
    Thread(target=main).start()

    # main()

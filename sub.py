import time
import zmq
from threading import Thread
from gateway_api import pub, sub

offset = 0


msg = {
    "offset": offset,
    "topic": "Queue-1"
}


def main():
    global offset
    # Prepare our context and publisher
    context = zmq.Context()
    publisher = context.socket(zmq.REQ)
    publisher.connect("tcp://localhost:5566")

    # sendgtwy = gateway_api.gateway()
    ack = sub(publisher, msg)

    # offset = msg["offset"]
    print ack

    publisher.close()
    context.term()


if __name__ == "__main__":
    Thread(target=main).start()
    Thread(target=main).start()

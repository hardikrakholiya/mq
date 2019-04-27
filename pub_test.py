import time
import zmq
import gateway_api as ga
from threading import Thread


def main():
    host = "localhost"
    port = "5565"
    topic = "Queue-1"
    msg = "Hello"
    socke = ga.gateway_api(host, port, topic)
    ack = socke.pub(msg)
    print ack


if __name__ == "__main__":
    for i in range(5):
        Thread(target=main).start()
import time
from threading import Thread
import gateway_api as ga


def main():
    host = "localhost"
    port = "5566"
    topic = "Queue-1"
    socke = ga.gateway_api(host, port, topic)
    ack = socke.sub()
    print ack


if __name__ == "__main__":
    Thread(target=main).start()
    # Thread(target=main).start()

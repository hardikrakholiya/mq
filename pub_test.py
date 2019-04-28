import time
import zmq
import gateway_api as ga
import json
import sys
from threading import Thread


def pub_test(ip, port):
    # print(ip, port)
    topic = "Queue-1"
    msg = "Hello,World!"
    publisher = ga.gateway_api(ip, port, topic)
    publisher.pub(msg)


if __name__ == "__main__":
    input_config = json.load(open(sys.argv[1]))
    for i in range(5):
        Thread(target=pub_test, args=(
            input_config["producer_gateway_ip"], input_config["producer_gateway_port"])).start()

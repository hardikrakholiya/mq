import time
import zmq
import gateway_api as ga
import json
import sys
from threading import Thread


def pub_test(ip, port):
    topic = "q2"
    msg = "Hello,World!"
    publisher = ga.gateway_api(ip, port, topic)
    publisher.pub(msg)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        input_config = json.load("config.json")
    else:
        input_config = json.load(open(sys.argv[1]))
    for i in range(1):
        Thread(target=pub_test, args=(
            input_config["producer_gateway_ip"], input_config["producer_gateway_port"])).start()

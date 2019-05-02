import time
from threading import Thread
import gateway_api as ga
import json
import sys
from datetime import datetime


def sub_test(ip, port, topic):
    consumer = ga.gateway_api(ip, port, topic)
    while True:
        msg = consumer.sub()
        if msg:
            print msg
        time.sleep(2)


if __name__ == "__main__":
    input_config = json.load(open(sys.argv[1]))
    topic = sys.argv[2]
    for i in range(1):
        Thread(target=sub_test, args=(
            input_config["consumer_gateway_ip"], input_config["consumer_gateway_port"], topic)).start()

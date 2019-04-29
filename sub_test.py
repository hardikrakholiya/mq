import time
from threading import Thread
import gateway_api as ga
import json
import sys


def sub_test(ip, port):
    # print(ip, port)
    topic = "Queue-1"
    consumer = ga.gateway_api(ip, port, topic)
    while True:
        msg = consumer.sub()
        print msg
        time.sleep(10)


if __name__ == "__main__":
    input_config = json.load(open(sys.argv[1]))
    for i in range(5):
        Thread(target=sub_test, args=(
            input_config["consumer_gateway_ip"], input_config["consumer_gateway_port"])).start()

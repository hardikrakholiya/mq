import zmq
import time


class gateway_api:
    def __init__(self, host, port, topic):
        self.topic = topic
        self.host = host
        self.port = port
        self.offset = 0
        self.context = zmq.Context()
        self.socc = self.context.socket(zmq.REQ)
        self.socc.connect("tcp://"+host+":"+port)

    def pub(self, msg):
        json_msg = {}
        json_msg["timestamp"] = time.time()
        json_msg["topic"] = self.topic
        json_msg["message"] = msg
        self.socc.send_json(json_msg)
        ack = self.socc.recv()
        return ack

    def sub(self):
        global offset
        json_msg = {}
        json_msg["offset"] = self.offset
        json_msg["topic"] = self.topic
        self.socc.send_json(json_msg)
        msg = self.socc.recv()
        print self.offset
        self.offset += 1
        return msg

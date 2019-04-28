import zmq
import time


class gateway_api:
    def __init__(self, host, port, topic):
        self.topic = topic
        self.host = host
        self.port = port
        self.offset = 0
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://"+host+":"+port)

    def pub(self, msg):
        json_msg = {}
        inner_msg = {}
        inner_msg["timestamp"] = time.time()
        inner_msg["text"] = msg
        # json_msg["op"] = "put"
        json_msg["topic"] = self.topic
        json_msg["msg"] = inner_msg
        self.socket.send_json(json_msg)
        ack = self.socket.recv()
        return ack

    def sub(self, offset=None):
        json_msg = {}
        # json_msg["op"] = "get"
        # In case user wants to specifiy an offset
        offset = offset if offset else self.offset
        json_msg["offset"] = offset
        json_msg["topic"] = self.topic
        self.socket.send_json(json_msg)
        msg = self.socket.recv_json()
        self.offset += 1
        return msg

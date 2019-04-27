def pub(publisher, msg):
    publisher.send_json(msg)
    ack = publisher.recv()
    return ack


def sub(publisher, msg):
    publisher.send_json(msg)
    msg = publisher.recv()
    return msg

def pub(publisher, msg):
    publisher.send_json(msg)
    ack = publisher.recv()
    return ack


offset = 0


def sub(publisher, msg):
    global offset
    msg["offset"] = offset
    publisher.send_json(msg)
    msg = publisher.recv()
    offset += 1
    return msg

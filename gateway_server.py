# from kazoo.client import KazooClient
import time
import zmq
from threading import Thread


def connect_to_broker(url, port, message):
    con = zmq.Context()
    socc = con.socket(zmq.REQ)
    socc.connect("tcp://"+url+":"+port)
    print "connected to broker"
    socc.send_json(message)
    ack = socc.recv()
    return ack


def worker_routine(url_worker, port):
    """Worker routine"""
    context = zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(url_worker)
    while True:
        message = socket.recv_json()
        # This will be fetched from ZK
        url = "149.160.227.28"
        port = "5555"
        ack = connect_to_broker(url, port, message)
        socket.send_json(ack)


def delegatejob(port, gtwy_name):
    url_worker = "inproc://workers"+gtwy_name
    url_gtwy = "tcp://*:"+port

    # Prepare our context and sockets
    context = zmq.Context.instance()

    # Socket to talk to clients
    gtwy = context.socket(zmq.ROUTER)
    gtwy.bind(url_gtwy)

    # Socket to talk to workers
    workers = context.socket(zmq.DEALER)
    workers.bind(url_worker)

    # Launch pool of worker threads
    for i in range(5):
        thread = Thread(
            target=worker_routine, args=(url_worker, port))
        thread.start()

    zmq.proxy(gtwy, workers)
    clients.close()
    workers.close()
    context.term()


def start_prod_gateway():
    delegatejob("5565", "prod_gateway")


def start_cons_gateway():
    delegatejob("5566", "cons_gateway")


if __name__ == "__main__":
    # zk = KazooClient(hosts='localhost:2181', read_only=True)
    # zk.start()
    Thread(target=start_prod_gateway).start()
    Thread(target=start_cons_gateway).start()

# from kazoo.client import KazooClient
import time
import zmq
from threading import Thread


def worker_routine(worker_url, gtwy_name, msg, context=None):
    """Worker routine"""
    context = context or zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(worker_url)
    while True:
        message = socket.recv_json()
        print "Received on"+gtwy_name+": ", message
        socket.send(msg)


def delegatejob(port, gtwy_name, msg):
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
            target=worker_routine, args=(url_worker, gtwy_name, msg))
        thread.start()

    zmq.proxy(gtwy, workers)
    clients.close()
    workers.close()
    context.term()


def prod_gtwy():
    delegatejob("5565", "prod_gtwy", "msg received by broker")


def cons_gtwy():
    delegatejob("5566", "con_gtwy", "msg sent from broker")


if __name__ == "__main__":
    # zk = KazooClient(hosts='localhost:2181', read_only=True)
    # zk.start()
    Thread(target=prod_gtwy).start()
    Thread(target=cons_gtwy).start()

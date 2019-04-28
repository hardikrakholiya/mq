# from kazoo.client import KazooClient
import time
import zmq
from threading import Thread


def connecttobroker(message):
    con = zmq.Context()
    socc = con.socket(zmq.REQ)
    socc.connect("tcp://149.160.227.28:5555")
    print "connected to broker"
    socc.send_json(message)
    # time.sleep(1)
    ack = socc.recv()
    return ack


def worker_routine(worker_url, gtwy_name, context=None):
    """Worker routine"""
    context = context or zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(worker_url)
    while True:
        message = socket.recv_json()
        print "message received"
        ack = connecttobroker(message)

        print "Received on"+gtwy_name+": ", ack
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
            target=worker_routine, args=(url_worker, gtwy_name))
        thread.start()

    zmq.proxy(gtwy, workers)
    clients.close()
    workers.close()
    context.term()


def prod_gtwy():
    delegatejob("5565", "prod_gtwy")


def cons_gtwy():
    delegatejob("5566", "con_gtwy")


if __name__ == "__main__":
    # zk = KazooClient(hosts='localhost:2181', read_only=True)
    # zk.start()
    Thread(target=prod_gtwy).start()
    Thread(target=cons_gtwy).start()

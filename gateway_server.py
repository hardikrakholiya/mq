# from kazoo.client import KazooClient
import time
import zmq
import json
import sys
from threading import Thread
from kazoo.client import KazooClient


def connect_to_broker(url, port, message):
    con = zmq.Context()
    socc = con.socket(zmq.REQ)
    socc.connect("tcp://"+url+":"+port)
    socc.send_json(message)
    ack = socc.recv()
    return ack


def worker_routine(zk, url_worker, port, input_config, gtwy_name):
    """Worker routine"""
    context = zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(url_worker)
    while True:
        message = socket.recv_json()
        master_broker = {}
        if zk.exists("topic/"+message["topic"]):
            # Get the master broker
            broker = zk.get_children(
                "/topic/"+message["topic"]+"/master")[0].decode("utf-8")
            broker_info = zk.get("/broker/"+broker)[0]
            master_broker = json.loads(broker_info.decode("utf-8"))
        elif gtwy_name != "cons_gateway":
            # Create a master broker and followers
            zk.ensure_path("/topic/"+message["topic"])
            context_controller = zmq.Context()
            controller_socket = context_controller.socket(zmq.REQ)
            controller_socket.connect(
                "tcp://"+input_config["controller_ip"]+":"+input_config["controller_port"])
            controller_socket.send_string(message["topic"])
            master_broker = json.loads(controller_socket.recv_json())
            print "<<<<master_broker>>>>", master_broker
            # print "json", zk.get(
            #     "/topic/"+message["topic"]+"/master/"+master_broker)
            # print "master_broker", master_broker

        # if zk.exists("/broker"):
        #     broker = zk.get_children("/broker")[0].decode("utf-8")
        #     broker_info, stat = zk.get("/broker/"+broker)
        #     broker_info = json.loads(broker_info.decode("utf-8"))
        ip = master_broker["address"]
        port = str(master_broker["port"])
        ack = connect_to_broker(ip, port, message)
        socket.send_json(ack)


def delegatejob(zk, gateway_port, zk_port, gtwy_name):
    url_worker = "inproc://workers"+gtwy_name
    url_gtwy = "tcp://*:"+gateway_port

    # Prepare our context and sockets
    context = zmq.Context.instance()

    # Socket to talk to clients
    gtwy = context.socket(zmq.ROUTER)
    gtwy.bind(url_gtwy)
    print("Starting gtwy on", url_gtwy)

    # Socket to talk to workers
    workers = context.socket(zmq.DEALER)
    workers.bind(url_worker)

    # Launch pool of worker threads
    for i in range(5):
        thread = Thread(
            target=worker_routine, args=(zk, url_worker, zk_port, input_config, gtwy_name))
        thread.start()

    zmq.proxy(gtwy, workers)
    clients.close()
    workers.close()
    context.term()


def start_prod_gateway(zk, input_config):
    delegatejob(zk, input_config["producer_gateway_port"],
                input_config["zk_port"], "prod_gateway")


def start_cons_gateway(zk, input_config):
    delegatejob(zk, input_config["consumer_gateway_port"],
                input_config["zk_port"], "cons_gateway")


if __name__ == "__main__":

    input_config = json.load(open(sys.argv[1]))
    zk = KazooClient(
        hosts=input_config["zk_ip"]+":"+input_config["zk_port"], read_only=True)
    zk.start()
    Thread(target=start_prod_gateway, args=(zk, input_config,)).start()
    print("Producer Gateway running on port",
          input_config["producer_gateway_port"])
    Thread(target=start_cons_gateway, args=(zk, input_config,)).start()
    print("Consumer Gateway running on port",
          input_config["consumer_gateway_port"])

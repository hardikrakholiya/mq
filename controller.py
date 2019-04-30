from kazoo.client import KazooClient
import json
import sys
import time
from threading import Lock
from threading import Thread
import zmq
from random import randrange
import random
import copy
import logging

mast_fol_map = {}


def assign_master(broker, topic):

    master_broker_index = randrange(0, len(broker))
    master_broker = broker[master_broker_index]

    print("Assigning master", master_broker_index, master_broker)
    broker.pop(master_broker_index)

    # Assign master
    zk.ensure_path("/topic/"+topic+"/master")
    master_broker_details = zk.get("/broker/"+master_broker)[0]
    master_broker_details = json.loads(
        master_broker_details.decode("utf-8"))
    master_broker_json = {}
    master_broker_json["address"] = master_broker_details["address"]
    master_broker_json["port"] = master_broker_details["port"]
    master_broker_json = json.dumps(master_broker_json)
    zk.create("/topic/"+topic+"/master/"+master_broker,
              master_broker_json)

    return master_broker, master_broker_json


def assign_followers(master_broker, broker, topic):
    global mast_fol_map
    # assign two followers
    print broker
    if len(broker) > 1:
        followers_index = random.sample(range(0, len(broker)), 2)
    elif len(broker) == 1:
        followers_index = random.sample(range(0, len(broker)), 1)
    else:
        print "here"
        return None
    followers = []
    for fol in followers_index:
        followers.append(broker[fol])

    print("Followers will be", followers)
    zk.ensure_path("/topic/"+topic+"/followers")
    for f in followers:
        follower_json = {}
        if zk.exists("/broker/"+f):
            follower_details = json.loads(
                zk.get("/broker/"+f)[0].decode("utf-8"))
        follower_json["address"] = follower_details["address"]
        follower_json["port"] = follower_details["port"]
        follower_json = json.dumps(follower_json)
        zk.create("/topic/"+topic+"/followers/"+f, follower_json)

    """Cache the Master and Followers"""
    return followers


def gateway_listener(zk, input_config):
    global mast_fol_map
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:"+input_config["controller_port"])
    while True:
        topic = socket.recv()
        # if broker exists we create a new master and followers for the same
        if zk.exists("/broker"):
            broker = zk.get_children("/broker")
            # assigning broker list.
            master_broker, master_broker_json = assign_master(broker, topic)
            followers = assign_followers(master_broker, broker, topic)
            if followers:
                if master_broker not in mast_fol_map:
                    mast_fol_map[master_broker] = {topic: followers}
                else:
                    mast_fol_map[master_broker][topic].append(followers)
            print "followers set"
            print "master_broker json", master_broker_json
            print "map", mast_fol_map
            socket.send_json(master_broker_json)


flag = [True]
prev_broker_list = []


def broker_watch(zk, input_config):
    # keep a watch on the changes of the broker list
    print("init")
    logging.basicConfig()

    @zk.ChildrenWatch("/broker")
    def watch_children(children):
        global flag
        global prev_broker_list
        global mast_fol_map

        print("flag", flag)
        if flag:
            print("Inside flag is false")
            prev_broker_list = copy.deepcopy(children)
            flag = []
        print "prev children", prev_broker_list
        print "current children", children
        if len(prev_broker_list) > len(children):
            for b in prev_broker_list:
                if b not in children:
                    print "down broker", b
                    for topic in mast_fol_map[b]:
                        print "this topic", topic
                        zk.delete(
                            "/topic/"+topic+"/master/"+b, recursive=True)
                        master_broker, master_broker_json = assign_master(
                            mast_fol_map[b][topic], topic)
                        new_followers = mast_fol_map[b][topic]
                        if master_broker in mast_fol_map[b][topic]:
                            new_followers = new_followers.remove(
                                master_broker)
                        print "new followers", new_followers
                        if master_broker not in mast_fol_map:
                            mast_fol_map[master_broker] = {
                                topic: new_followers}

                        else:
                            mast_fol_map[master_broker][topic] = new_followers

                        if b in mast_fol_map:
                            zk.delete("/topic/"+topic+"/followers/" +
                                      master_broker, recursive=True)
                            mast_fol_map.pop(b, None)

        elif len(prev_broker_list) < len(children):
            print "trying to set new follower"
            new_broker = None
            for b in children:
                if b not in prev_broker_list:
                    new_broker = b
            for master_b in mast_fol_map:
                for topic in mast_fol_map[master_b]:
                    print mast_fol_map[master_b][topic]
                    if len(mast_fol_map[master_b][topic]) < 2:
                        followers = assign_followers(
                            master_b, [new_broker], topic)
                        if followers:
                            if master_b not in mast_fol_map:
                                mast_fol_map[master_b] = {
                                    topic: followers}
                            else:
                                mast_fol_map[master_b][topic].append(
                                    followers[0])

        print "mast_fol_map", mast_fol_map
        prev_broker_list = children[:]

        print("Children are now: %s" % children)

    while True:
        time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        input_config = json.load("config.json")
    else:
        input_config = json.load(open(sys.argv[1]))
    zk = KazooClient(
        hosts=input_config["zk_ip"]+":"+input_config["zk_port"], read_only=True)
    zk.start()
    Thread(target=broker_watch, args=(zk, input_config)).start()
    Thread(target=gateway_listener, args=(zk, input_config)).start()

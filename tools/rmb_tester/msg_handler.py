#!/usr/bin/env python3

import redis
from rmb_tester import Message
import time
import argparse

def listen(q):
    count = 0
    while True:
        result = r.blpop(f'msgbus.{q}')
        msg = Message.from_json(result[1])
        msg.epoch = int(time.time())
        msg.twin_dst, msg.twin_src = msg.twin_src, msg.twin_dst
        r.lpush(msg.reply_to, msg.to_json())
        count += 1
        print(f"Responses sent out: {count}", end='\r')

if __name__ == "__main__":
    parser = argparse.ArgumentParser("RMB_echo")
    parser.add_argument("-q", "--queue", help="redis queue name. defaults to 'testme'",  type=str, default='testme')
    parser.add_argument("-p", "--redis-port", help="redis port for the instance used by rmb-peer", type=int, default=6380)
    args = parser.parse_args()

    r = redis.Redis(host='localhost', port=args.redis_port, db=0)
    print("RMB_echo")
    print(f"handling command msgbus.{args.queue}")
    listen(args.queue)

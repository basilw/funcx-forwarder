import redis
from redis.exceptions import ConnectionError
import queue
import json
import time
from typing import Tuple
from funcx_forwarder.errors import FuncxError
from funcx_forwarder.queues.redis.tasks import Task, TaskState
import logging

logger = logging.getLogger(__name__)

class NotConnected(FuncxError):
    """ Queue is not connected/active
    """
    def __init__(self, queue):
        self.queue = queue

    def __repr__(self):
        return "Queue {} is not connected. Cannot execute queue operations".format(self.queue)


class RedisPubSub(object):

    def __init__(self,
                 hostname: str,
                 port: int = 6379):
        self.hostname = hostname
        self.port = port
        self.subscriber_count = 0
        self.redis_client = None
        self.task_channel_prefix = 'task_channel_'
        self._task_channel_prefix_len = len(self.task_channel_prefix)
        self.task_queue_prefix = 'task_queue_'

    def connect(self):
        try:
            self.redis_client = redis.StrictRedis(host=self.hostname, port=self.port, decode_responses=True)
            self.redis_client.ping()
        except ConnectionError:
            logger.exception("ConnectionError while trying to connect to Redis@{}:{}".format(
                self.hostname,
                self.port))
            raise
        self.pubsub = self.redis_client.pubsub()

    def put(self, endpoint_id, task):
        """ Put the task object into the channel for the endpoint
        """
        try:
            task.endpoint = endpoint_id
            task.status = TaskState.WAITING_FOR_EP
            # Note: Task object is already in Redis
            # self.redis_client.hset(f'task_{task.task_id}', kind, json.dumps(payload))
            subscribers = self.redis_client.publish(f'{self.task_channel_prefix}{endpoint_id}', task.task_id)
            if subscribers == 0:
                self.redis_client.rpush(f'{self.task_queue_prefix}{endpoint_id}', task.task_id)
                # logger.debug("No active subscribers. Pushing to queue")
            #logger.debug(f"Active subscribers : {subscribers}")

        except AttributeError:
            raise Exception("Not connected")
            raise NotConnected(self)

        except ConnectionError:
            logger.exception("ConnectionError while trying to connect to Redis@{}:{}".format(
                self.hostname,
                self.port))
            raise

    def republish_from_queue(self, endpoint_id):
        """ Tasks pushed to Redis pubsub channels might have gone unreceived.
        When a new endpoint registers, it should republish tasks from it's queues
        to the pubsub channels.
        """
        while True:
            try:
                x = self.redis_client.blpop(f'{self.task_queue_prefix}{endpoint_id}', timeout=1)
                if not x:
                    break
                task_list, task_id = x
                self.redis_client.publish(f'{self.task_channel_prefix}{endpoint_id}', task_id)
            except AttributeError:
                logger.exception("Failure while republishing from queue to pubsub")
                raise NotConnected(self)

            except redis.exceptions.ConnectionError:
                logger.exception("Failure while republishing from queue to pubsub")
                raise

    def subscribe(self, endpoint_id):
        logger.info(f"Subscribing to tasks_{endpoint_id}")
        self.pubsub.subscribe(f'{self.task_channel_prefix}{endpoint_id}')
        self.republish_from_queue(endpoint_id)
        self.subscriber_count += 1

    def unsubscribe(self, endpoint_id):
        self.pubsub.unsubscribe(f'{self.task_channel_prefix}{endpoint_id}')
        self.subscriber_count -= 1

    def get(self, timeout: int = 2) -> Tuple[str, Task]:
        """
        Parameters
        ----------

        timeout : int
             milliseconds to wait
        """
        if self.subscriber_count < 1:
            raise queue.Empty

        timeout_s = timeout / 1000
        try:
            package = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout_s)
            if not package:
                raise queue.Empty("Channels empty")
            # Strip channel prefix
            dest_endpoint = package['channel'][self._task_channel_prefix_len:]
            task_id = package['data']
            task = Task.from_id(self.redis_client, task_id)

        except queue.Empty:
            raise

        except AttributeError:
            raise NotConnected(self)

        except ConnectionError:
            logger.exception(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

        except Exception:
            logger.exception("Uncaught exception")

        return dest_endpoint, task


def test_client(ep=None):
    import time
    receiver = RedisPubSub('127.0.0.1')
    receiver.connect()
    receiver.subscribe(ep)

    t = None
    delta = None
    count = 0
    while True:
        try:
            msg = receiver.get(timeout=10)

            dest_endpoint, task = msg
            print("Received msg : ", msg)
            if not t:
                t = time.time()
            if task_id == 'sentinel':
                print("Breaking received sentinel")
                delta = time.time() - t
                break
            else:
                count += 1
        except Exception:
            pass

    print("Performance : {:8.3f} Msgs/s".format(count/delta))

def test_server(ep=None, count=1000, delay=0):
    sender = RedisPubSub('127.0.0.1')
    sender.connect()
    task = Task(sender.redis_client, 'task.0', container='RAW', serializer='', payload='Primer')
    sender.put(ep, task)
    for i in range(count):
        task = Task(sender.redis_client, f'task.{i}', container='RAW', serializer='',
                    payload=f'task body for task.{i}')
        sender.put(ep, task)
        time.sleep(delay)

    task = Task(sender.redis_client, f'sentinel', container='RAW', serializer='',
                payload='sentinel')
    sender.put(ep, task)


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client", action='store_true',
                        help="Run in client mode")
    parser.add_argument("-e", "--endpoint_id", default='testing_ep1',
                        help="Run in client mode")

    args = parser.parse_args()

    if args.client == True:
        test_client(ep=args.endpoint_id)
    else:
        test_server(ep=args.endpoint_id, count=10, delay=10)


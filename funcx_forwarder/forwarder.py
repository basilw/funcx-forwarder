import logging
import os
import zmq
import queue
import threading
import funcx_forwarder
from funcx_forwarder import set_file_logger, set_stream_logger


from multiprocessing import Process, Queue, Event
from funcx_forwarder.taskqueue import TaskQueue
from funcx_forwarder.queues.redis.redis_pubsub import RedisPubSub
from funcx_endpoint.executors.high_throughput.messages import Task, Heartbeat

from funcx_forwarder.queues.redis.tasks import Task as RedisTask
from funcx_forwarder.queues.redis.tasks import TaskState
import time
import pickle

logger = None


loglevels = {50: 'CRITICAL',
             40: 'ERROR',
             30: 'WARNING',
             20: 'INFO',
             10: 'DEBUG',
             0: 'NOTSET'}


class Forwarder(Process):
    """

                                  +-------------------------+
                                  |         Redis           |
                                  +-----------|-------------+
                                              |
                                  +-----------V-------------+
                                  |                         |
    forwarder-service ---reg_ep---|->      Forwarder        |
                                  |                         |
                                  +-------------------------+
                                        |     ^       ||
                                        |     |       ||
                                     Tasks Results Commands
                                        |     |       ||
                                        V     |       ||
                  +-------------+     +--------------------+
                  |  Endpoint 1 | ... |     Endpoint N     |
                  +-------------+     +--------------------+
    """

    def __init__(self,
                 command_queue,
                 response_queue,
                 address: str,
                 redis_address: str,
                 endpoint_ports=(55001, 55002, 55003),
                 redis_port: int = 6379,
                 logdir: str = "forwarder_logs",
                 stream_logs: bool = False,
                 logging_level=logging.INFO,
                 heartbeat_period=2,
                 keys_dir=os.path.abspath('.curve'),
                 poll_period: int = 10):
        """
        Parameters
        ----------
        ep_registration_queue: Queue
             multiprocessing queue over which the service will send requests to register
             new endpoints with the forwarder

        stream_logs: Bool
             When enabled, forwarder will stream logs to STDOUT/ERR.

        address : str
             Public address at which the forwarder will be accessible from the endpoints

        redis_address : str
             full address to connect to redis. Required

        redis_port : int
             redis port. Defaults to 6379

        poll_period : int
             poll_period in milliseconds
        """
        super().__init__()
        self.command_queue = command_queue
        self.response_queue = response_queue
        self.address = address
        self.redis_url = f"{redis_address}:{redis_port}"
        self.logdir = logdir
        self.tasks_port, self.results_port, self.commands_port = endpoint_ports
        self.connected_endpoints = {}
        self.kill_event = Event()
        self.heartbeat_period = heartbeat_period
        self._last_heartbeat = time.time()
        self.endpoint_registry = {}
        self.keys_dir = keys_dir

        self.redis_pubsub = RedisPubSub(hostname=redis_address, port=redis_port)

        global logger
        if self.logdir:
            os.makedirs(self.logdir, exist_ok=True)
            logger = set_file_logger(f'{self.logdir}/forwarder.log', level=logging_level)

        if stream_logs:
            logger = set_stream_logger(level=logging_level)

        logger.info(f"Initializing forwarder v{funcx_forwarder.__version__}")
        logger.info(f"Forwarder running on public address: {self.address}")
        logger.info(f"REDIS url: {self.redis_url}")
        logger.info("Log level set to {}".format(loglevels[logging_level]))

    def command_processor(self, kill_event):
        """ command_processor listens on the self.command_queue
        for commands and responds with results on the self.response_queue

        COMMAND messages are dicts of the form:
        {'command' : ['TERMINATE', 'REGISTER_ENDPOINT' ... 'ENDPOINT_LOAD_CONFIG'],
         'id': <ID:int>,
         'options' : ...
        }
        Responses are of the form:
        """
        while not kill_event.is_set():
            command = self.command_queue.get()
            logger.debug(f"[COMMAND] Received command {command}")
            if command['command'] == 'LIVENESS':
                response = {'response': True,
                            'id': command.get('id')}
            elif command['command'] == 'TERMINATE':
                logger.info("[COMMAND] Received TERMINATE command")
                response = {'response': True,
                            'id': command.get('id')}
                kill_event.set()
            elif command['command'] == 'ADD_ENDPOINT_TO_REGISTRY':
                logger.info("[COMMAND] Received REGISTER_ENDPOINT command")
                result = self.add_endpoint_to_registry(command['endpoint_id'],
                                                       command['client_public_key'])

                response = {'response': result,
                            'id': command.get('id'),
                            'endpoint_id': command['endpoint_id'],
                            'public_ip': self.address,
                            'tasks_port': self.tasks_port,
                            'results_port': self.results_port,
                            'commands_port': self.commands_port}

            else:
                response = {'response': False,
                            'id': command.get('id'),
                            'reason': 'Unknown command'}

            self.response_queue.put(response)

    def add_endpoint_to_registry(self, endpoint_id, key):
        """ Add new client keys to the zmq authenticator

        Registering an existing endpoint_id is allowed
        """
        logger.info(f"Endpoint_id:{endpoint_id} added to registry")
        self.endpoint_registry[endpoint_id] = {'creation_time': time.time(),
                                               'client_public_key': key}
        self.tasks_q.add_client_key(endpoint_id, key)
        self.results_q.add_client_key(endpoint_id, key)
        self.commands_q.add_client_key(endpoint_id, key)
        return True

    def unregister_endpoint(self, endpoint_id):
        """ Unsubscribes from Redis pubsub and "removes" endpoint from the tasks channel

        Triggered by either heartbeats or tasks not getting delivered
        TODO: This needs some extensive testing. It is unclear how well detecting failures
        will work on WAN networks with latencies.
        """
        logger.debug(f"Unregistering endpoint: {endpoint_id}")
        self.redis_pubsub.unsubscribe(endpoint_id)
        # TODO: YADU Combine the registry with connected_endpoints
        self.endpoint_registry.pop(endpoint_id, None)
        self.connected_endpoints.pop(endpoint_id, None)

    def add_endpoint_keys(self, ep_id, ep_key):
        """ To remove. this is not used.
        """
        self.tasks_q.add_client_key(ep_key)
        self.results_q.add_client_key(ep_key)
        self.commands_q.add_client_key(ep_key)

    def add_subscriber(self, ep_id):
        self.redis_pubsub.subscribe(ep_id)

    def heartbeat(self):
        """ ZMQ contexts are not thread-safe, heartbeats should happen on the same thread.
        """
        if self._last_heartbeat + self.heartbeat_period > time.time():
            return
        logger.debug("Heartbeat")
        dest_endpoint_list = list(self.connected_endpoints.keys())
        for dest_endpoint in dest_endpoint_list:
            logger.debug(f"Sending heartbeat to {dest_endpoint}")
            msg = Heartbeat(endpoint_id=dest_endpoint)
            try:
                self.tasks_q.put(dest_endpoint.encode('utf-8'),
                                 msg.pack())
                self.connected_endpoints[dest_endpoint]['missed_heartbeats'] = 0

            except (zmq.error.ZMQError, zmq.Again):
                logger.exception(f"Endpoint:{dest_endpoint} is unreachable over heartbeats")
                self.unregister_endpoint(dest_endpoint)
        self._last_heartbeat = time.time()

    def run(self):
        """ Process entry point
        """
        logger.info("[MAIN] Loop starting")
        logger.info("[MAIN] Connecting to redis")
        logger.info(f"[MAIN] Forwarder listening for tasks on: {self.tasks_port}")
        logger.info(f"[MAIN] Forwarder listening for results on: {self.results_port}")
        logger.info(f"[MAIN] Forwarder issuing commands on: {self.commands_port}")
        try:
            self.redis_pubsub.connect()
        except Exception:
            logger.exception("[MAIN] Failed to connect to Redis")
            raise

        # TODO : THis timeout might become an issue
        # TaskQueue in server mode binds to all interfaces
        self.tasks_q = TaskQueue('127.0.0.1', port=self.tasks_port, RCVTIMEO=1, mode='server')
        self.results_q = TaskQueue('127.0.0.1', port=self.results_port, mode='server')
        self.commands_q = TaskQueue('127.0.0.1', port=self.commands_port, mode='server')

        self._command_processor_thread = threading.Thread(target=self.command_processor,
                                                          args=(self.kill_event,),
                                                          name="forwarder-command-processor")
        self._command_processor_thread.start()

        """  DEBUG : We will initially hack this with just one named endpoint `testing_ep1`
        This should happen on trigger from a registration request from the service
        We might not use this at all if we end up using the stone house pattern alone.
        """
        # self.add_endpoint_keys('testing_ep1', '/tmp/client.key')

        while True:

            if self.kill_event.is_set():
                logger.critical("Kill event set. Starting termination sequence")
                # 1. Unsubscribe from all
                # 2. Flush all tasks received back to their queues for reprocessing.
                # 3. [TODO] Figure out how we can trigger a scaling event to replace lost forwarder?

            # TODO : Add heartbeat messages
            # Send heartbeats to every connected manager
            self.heartbeat()

            # Receive endpoint registration messages. Only registration messages
            # are sent from the interchange -> forwarder on the task_q
            try:
                b_ep_id, reg_message = self.tasks_q.get(timeout=0)  # timeout in ms # Update to 0ms
                # At this point ep_id is authenticated by means having the client keys.
                ep_id = b_ep_id.decode('utf-8')
                logger.info(f'Endpoint:{ep_id} connected')

                if ep_id in self.connected_endpoints:
                    # This really shouldn't happen, could be a reconnect ?
                    logger.warning(f"[MAIN] Endpoint:{ep_id} attempted connect when it already is in connected list")
                self.connected_endpoints[ep_id] = {'registration_message': reg_message,
                                                   'missed_heartbeats': 0}

                # Now subscribe to messages for ep_id
                self.add_subscriber(ep_id)
            except zmq.Again:
                pass
            except Exception:
                logger.exception("Caught exception while waiting for registration")

            # Now wait for any messages on REDIS that needs forwarding.
            task = None
            try:
                dest_endpoint, task = self.redis_pubsub.get(timeout=0)
                logger.debug(f"Got message from REDIS: {dest_endpoint}:{task}")
            except queue.Empty:
                # logger.debug("REDIS queues are empty")
                pass
            except Exception:
                logger.exception("Caught exception waiting for message from REDIS")
                pass

            if task:
                if dest_endpoint not in self.endpoint_registry:
                    # At this point we should be unsubscribed and receiving only messages
                    # from the TCP buffers.
                    self.redis_pubsub.put(dest_endpoint, task)
                else:
                    try:
                        logger.info(f"Sending task:{task.task_id} to endpoint:{dest_endpoint}")
                        zmq_task = Task(task.task_id,
                                        task.container,
                                        task.payload)
                        self.tasks_q.put(dest_endpoint.encode('utf-8'),
                                         zmq_task.pack())
                    except (zmq.error.ZMQError, zmq.Again):
                        logger.exception(f"Endpoint:{dest_endpoint} is unreachable")
                        self.unregister_endpoint(dest_endpoint)
                    except Exception:
                        logger.exception("Caught error while sending {task.task_id} to {dest_endpoint}")
                        pass

            try:
                # timeout in ms # Update to 0ms
                b_ep_id, b_message = self.results_q.get(block=False, timeout=1)
                # logger.debug(f"Message from results_q : {b_message}")
                try:
                    message = pickle.loads(b_message)
                except Exception:
                    logger.exception(f"Failed to unpickle message from results_q, message:{b_message}")

                if 'registration' in message:
                    logger.debug(f"Registration message from {message['registration']}")
                    continue

                task = RedisTask.from_id(self.redis_pubsub.redis_client, message['task_id'])

                # TODO: What does the res_dict look like?  Can we just set task.result=res_dict?
                if 'result' in message:
                    task.status = TaskState.SUCCESS
                    task.result = message['result']
                    task.completion_time = time.time()
                    # TODO:Yadu remove the log line
                    logger.debug(f"Result : {message['task_id']} {message['result']}")
                elif 'exception' in message:
                    task.status = TaskState.FAILED
                    task.exception = message['exception']
                    task.completion_time = time.time()
                    # TODO:Yadu remove the log line
                    logger.debug(f"Exception : {message['task_id']} {message['exception']}")

            except zmq.Again:
                # logger.debug("[MAIN] No result messages")
                pass
            except Exception:
                logger.exception("Caught exception from results queue")


if __name__ == '__main__':

    command, response = Queue(), Queue()
    fw = Forwarder(command, response, '127.0.0.1')
    # fw.start()
    fw.run()

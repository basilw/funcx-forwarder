import logging
import os
import zmq
import requests
import redis
from funcx_forwarder import set_file_logger

from multiprocessing import Process, Queue
from taskqueue import TaskQueue

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
                 redis_address : str,
                 endpoint_ports=(55001, 55002, 55003),

                 redis_port: int = 6379,
                 logdir: str = "forwarder_logs",
                 logging_level=logging.INFO,
                 heartbeat_period=60,
                 poll_period: int = 10,
                 max_heartbeats_missed=2):
        """
        Parameters
        ----------
        ep_registration_queue: Queue
             multiprocessing queue over which the service will send requests to register
             new endpoints with the forwarder

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
        self.redis_url = f"{redis_address}:{redis_port}"
        self.logdir = logdir
        self.tasks_port, self.results_port, self.commands_port = endpoint_ports

        os.makedirs(self.logdir, exist_ok=True)

        global logger
        logger = set_file_logger(f'{self.logdir}/forwarder.log')

        logger.info("Initializing forwarder")
        logger.info("Log level set to {}".format(loglevels[logging_level]))


    def register_endpoint(self):
        """ Add new client keys to the zmq authenticator
        """

        pass

    def run(self):
        """ Process entry point
        """
        logger.info("[MAIN] Loop starting")
        logger.info(f"[MAIN] Connecting to redis")
        logger.info(f"[MAIN] Forwarder listening for Tasks on :{self.tasks_port}")
        logger.info(f"[MAIN] Forwarder listening for Results on :{self.results_port}")
        logger.info(f"[MAIN] Forwarder issuing for Commands on :{self.commands_port}")

        tasks_q = TaskQueue('127.0.0.1', port=55001, mode='server')
        zmq_context = tasks_q.zmq_context()
        print("ZMQ_Context : ", zmq_context)
        results_q = TaskQueue('127.0.0.1', port=55002, mode='server')
        commands_q = TaskQueue('127.0.0.1', port=55003, mode='server')

        while True:
            ep, message = results_q.get()
            logger.info(f"Got message : {message}")
            tasks_q.put(ep, b'hello from server')
            logger.info("Sending response message")


        


        """
        # Thread 1
        while True:
            ep, message = get_message_from_redis(<from_all_subscribed_channels>)
            try:
               forward(ep, message)
            except HostUnreachable?
               trigger_reconnect logic.

        # Thread 2
        while True:
            ep, message = recv_from_endpoints()
            update_redis(ep, message)

        # Thread 3
        while True:
            listen to service queue and dynamically add auth

        """


if __name__ == '__main__':

    command, response = Queue(), Queue()
    fw = Forwarder(command, response, '127.0.0.1')
    fw.start()

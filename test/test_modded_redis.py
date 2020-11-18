import argparse
import time
import uuid

from funcx.serialize import FuncXSerializer

# from funcx_forwarder.queues import EndpointQueue
from funcx_forwarder.queues.redis.tasks import Task, TaskState
from funcx_forwarder.queues.redis.redis_pubsub import RedisPubSub

def slow_double(i, duration=0):
    import time
    time.sleep(duration)
    return i * 2


def dont_run_yet(endpoint_id=None, tasks=10, duration=1, hostname=None):
    # tasks_rq = EndpointQueue(f'task_{endpoint_id}', hostname)
    tasks_channel = RedisPubSub(hostname)
    tasks_channel.connect()
    redis_client  = tasks_channel.redis_client
    redis_client.ping()
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    start = time.time()
    task_ids = {}
    for i in range(tasks):
        time.sleep(duration)
        task_id = str(uuid.uuid4())
        print("Task_id : ", task_id)
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({'duration': duration})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        container_id = "RAW"
        task = Task(redis_client, task_id, container_id, serializer="", payload=payload)
        task.endpoint = endpoint_id
        task.status = TaskState.WAITING_FOR_EP
        # tasks_rq.enqueue(task)
        tasks_channel.put(endpoint_id, task)
        task_ids[i] = task_id

    d1 = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(tasks, d1))

    delay = 5
    print(f"Sleeping {delay} seconds")
    time.sleep(delay)
    print(f"Launched {tasks} tasks")
    for i in range(tasks):
        task_id = task_ids[i]
        print("Task_id : ", task_id)
        task = Task.from_id(redis_client, task_id)
        # TODO: wait for task result...
        time.sleep(duration)
        try:
            result = fxs.deserialize(task.result)
            print(f"Result : {result}")
        except Exception as e:
            print(f"Task failed with exception:{e}")
            pass

    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(tasks / delta))
    return delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--redis_hostname", default='127.0.0.1',
                        help="Hostname of the Redis server")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Endpoint_id")
    parser.add_argument("-d", "--duration", required=True,
                        help="Duration of the tasks")
    parser.add_argument("-c", "--count", required=True,
                        help="Number of tasks")

    args = parser.parse_args()
    dont_run_yet(endpoint_id=args.endpoint_id,
                 hostname=args.redis_hostname,
                 duration=int(args.duration),
                 tasks=int(args.count))

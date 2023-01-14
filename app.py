from argparse import ArgumentParser
import json
from time import time
from typing import List, Union
from kafka import KafkaConsumer, KafkaProducer

from utils import Env, Const, Frame, MetricsCalculator


# TODO:
"""
- docstrings
- Show results: 1 minute + Latency 5 sec;
- bool verbose
"""


def run(src_messages: Union[KafkaConsumer, List], producer, verbose: bool):

    total_messages = 0
    received_frames = []
    i = -1
    sec_interval_pointers = []
    min_interval_pointers = []
    out_of_order_frames = []

    total_time_in_sec = 0
    prev_time = 0

    msg_processing_time = 0
    exec_start_time = time()
    while True:
        try:
            for message in src_messages:
                total_messages += 1
                msg_proc_start_time = time()
                try:
                    message_value = message.value

                    assert Const.TIMESTAMP in message_value and Const.UID in message_value, \
                        f'Fields {Const.TIMESTAMP} or {Const.UID} not present in message value.'
                    # producer.send(Env.OUTPUT_STREAM, message_value)
                    timestamp = message_value[Const.TIMESTAMP]
                    uid = message_value[Const.UID]

                    assert isinstance(timestamp, int) and isinstance(uid, str), \
                        f"Invalid data type: {Const.TIMESTAMP} must be integer, got {type(timestamp)};\
                        {Const.UID} must be string, got {type(uid)}"
                except Exception as e:
                    print(e)
                    continue

                frame = Frame(timestamp, uid)

                i += 1
                received_frames.append(frame)

                if not prev_time:
                    # First frame arrives; set sec and minute pointers
                    prev_time = frame.timestamp
                    sec_interval_pointers.append(i)
                    min_interval_pointers.append(i)
                    continue

                time_delta = frame.timestamp-prev_time

                if time_delta > 0:
                    # The case when time_delta>1 is not covered
                    # TODO: propose solution;
                    # TODO: propose solution for out of order frames;
                    prev_time = frame.timestamp
                    total_time_in_sec += time_delta
                    sec_interval_pointers.append(i)

                    metrics_per_second = MetricsCalculator.calculate_per_second_metric(
                        received_frames, sec_interval_pointers)
                    producer.send(
                        topic=Env.SEC_METRICS_OUTPUT_TOPIC, value=metrics_per_second)
                    # This would make producers synchronous and reduce throughput
                    # producer.flush()

                    if verbose:
                        print(
                            f"Produced message to topic {Env.SEC_METRICS_OUTPUT_TOPIC}")
                        print(
                            f"{metrics_per_second['datetime']}: {metrics_per_second['num_of_frames']} frames.")
                        print(
                            f"Average {metrics_per_second['avg_num_of_frames']} frames/second.")

                    if total_time_in_sec % Const.SEC_IN_MIN == 0:
                        min_interval_pointers.append(i)
                        metrics_per_minute = MetricsCalculator.calculate_unique_users(
                            received_frames, min_interval_pointers)
                        producer.send(
                            topic=Env.MIN_METRICS_OUTPUT_TOPIC, value=metrics_per_minute)

                        if verbose:
                            print(
                                f"Produced message to topic {Env.MIN_METRICS_OUTPUT_TOPIC}")
                            print(
                                f"{metrics_per_minute['datetime']}: {metrics_per_minute['unique_users']} unique users.")
                            print(
                                f"Average {metrics_per_minute['avg_users_per_minute']} users/minute.")
                        # This would make producers synchronous and reduce throughput
                        # producer.flush()

                elif time_delta < 0:
                    out_of_order_frames.append(received_frames.pop())
                    i -= 1

                msg_processing_time += time()-msg_proc_start_time
                if total_messages % 10000 == 0 and verbose:
                    print(
                        f'Processed {total_messages} messages in {round(time()-exec_start_time, 2)} seconds')
                    print(
                        f"Average message processing time: {msg_processing_time/total_messages} seconds.")
                    print(
                        f"Out of order frames: {len(out_of_order_frames)}/{total_messages}")
        finally:
            consumer.close()
            producer.flush()
            producer.close()


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--no-verbose', dest='verbose', action='store_false')
    parser.set_defaults(verbose=True)

    args = parser.parse_args()

    consumer = KafkaConsumer(
        Env.INPUT_TOPIC,
        bootstrap_servers=Env.BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=Env.CONSUMER_GROUP_ID+'_1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=Env.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=Const.LATENCY*1000
    )

    run(consumer, producer, args.verbose)

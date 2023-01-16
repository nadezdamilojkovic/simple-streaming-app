from collections import namedtuple
from datetime import datetime
import math
import os
import logging
import logging.config
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()


def configure_logger():
    logging.basicConfig(filename='logging/log.txt',
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })
    logger = logging.getLogger("StreamingApp")
    logger.addHandler(logging.StreamHandler())
    return logger


Frame = namedtuple("Frame", ["timestamp", "uid"])


class Env:
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS").split(',')
    CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC")
    SEC_METRICS_OUTPUT_TOPIC = os.getenv("SEC_METRICS_OUTPUT_TOPIC")
    MIN_METRICS_OUTPUT_TOPIC = os.getenv("MIN_METRICS_OUTPUT_TOPIC")
    OUTPUT_STREAM = os.getenv("OUTPUT_STREAM")
    PRINT_INTERVAL = int(os.getenv("PRINT_INTERVAL"))


class Const:
    TIMESTAMP = "ts"
    UID = "uid"
    LATENCY = 5000
    SEC_IN_MIN = 60


def timestamp_to_formated_datetime(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


class MetricsCalculator:
    sum_frames_per_second = 0
    sum_users_per_minute = 0

    @classmethod
    def calculate_per_second_metric(
        cls,
        frames: List,
        seconds_pointers: List
    ) -> Dict[str, float]:
        """
            Calculate metrics for the last second. 
            Frames arrived in the last second are accessed by slicing frames list with the last two
            pointers in the seconds_pointers list;

            Args:
                frames (list): a list of received frames;
                seconds_pointers (list): a list of indices of the first frame in each consecutive second

            Returns:
                Dict[str, float]: per second statistics as a dict
        """
        sec_range_start = seconds_pointers[-2]
        sec_range_end = seconds_pointers[-1]

        timestamp = frames[sec_range_start].timestamp
        formated_datetime = timestamp_to_formated_datetime(timestamp)
        num_of_frames = sec_range_end-sec_range_start

        cls.sum_frames_per_second += num_of_frames
        avg_frames_per_second = math.ceil(
            cls.sum_frames_per_second / (len(seconds_pointers)-1))

        metrics = {
            "timestamp": timestamp,
            "datetime": formated_datetime,
            "num_of_frames": num_of_frames,
            "avg_num_of_frames": avg_frames_per_second
        }

        return metrics

    @classmethod
    def calculate_unique_users(
        cls,
        frames: List,
        mins_pointers: List
    ) -> Dict[str, float]:
        """
            Calculate metrics for the last minute. 
            Frames arrived in the last minute are accessed by slicing frames list with the last two
            pointers in the mins_pointers list;

            Args:
                frames (list): a list of received frames;
                seconds_pointers (list): a list of indices of the first frame in each consecutive minute

            Returns:
                Dict[str, float]: per minute statistics as a dict
        """
        min_range_start = mins_pointers[-2]
        min_range_end = mins_pointers[-1]

        users_in_last_minute = set(
            [frame.uid for frame in frames[min_range_start:min_range_end]])

        timestamp = frames[min_range_start].timestamp

        formated_datetime = timestamp_to_formated_datetime(timestamp)
        users_count = len(users_in_last_minute)
        cls.sum_users_per_minute += users_count
        avg_users_per_minute = math.ceil(
            cls.sum_users_per_minute/len(mins_pointers)-1)
        metrics = {
            "timestamp": timestamp,
            "datetime": formated_datetime,
            "unique_users": users_count,
            "avg_users_per_minute": avg_users_per_minute
        }

        return metrics

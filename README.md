# simple-streaming-app

* This repository contains the code for the simple streaming application with dockerized Kafka services.  
* *docker-compose.yml* is used to manage required services:          
  * **zookeeper**;
  * Kafka Brokers:
    * **broker**:  
    Confluent server image that includes Kafka and is fully compatible with it;
    * **prepare-topics**:  
    this container is build from confluent server image, in order to access kafka's scripts to create topics (kafka-topics) and produce data to the topic using kafka's. It will be stopped after the data from stream.jsonl is produced to the INPUT_STREAM topic.
  * **control-center**:  
  this container will instantiate Control-Center, which will be available on the *localhost:9021* and can be used for easier managing and monitoring of the Kafka cluster (broker's resources, topics and their messages, consumers,...).

## Solution overview
The execution entry point in the app.py script is the run() method that will consumer messages from the kafka topic indefinitely. The format of the input messages from the kafka topic is expected to be a json object with the mandatory fields *'timestamp'* and *'uid'*, which is validated immidiately after the message is consumed from the topic. 
A list of valid, received frames is maintained and is indexed with a variable i. Additional lists *sec_interval_pointers* and *min_interval_pointers* will contain pointers (indices) to the first frame from each consecutive second and minute, respectivelly. Total time in seconds of received frames is also maintained through the consumer loop.
Successive order of received frames is ensured by calculating time delta of the last received frame and current consumed frame. There are 3 possible cases when processing consumed frame:
   - **time_delta == 0**: current frame is from the same second as the last received frame, only append it to the list of received frames;
   - **time_delta > 0**: the frame is from the next second, therefore, the per-second statistics will be calculated (frames per second) and produced to the corresonding output topic and total time will be incremented by time_delta. The sec_interval_pointers will be updated accordingly. In addition to that, when the minute ends, the min_interval_pointers is updated with the current index i, the number of unique users of the passed minute is calculated and produced to the output topic; DISCLAIMER: current implementation does not cover time_delta>1, but the fix would be to add a multiple pointers in sec_interval_pointers (and min_interval_pointers, if needed) pointing to the same index i;
   - **time_delta < 0**: in the current implementation, out of order frames will be discarded from the received_frames list, but the solution to handle them would be to alter the received_frames list and insert them at the approptiate index as they arrive.

## Required:
1. python 3.8
2. pipenv
3. docker
4. docker-compose


## Environment variables
| Environment variable        | Description           |
| --------------------------- |:---------------------:|
| BOOTSTRAP_SERVERS           | comma separated list of kafka brokers to act as bootstrap servers|
| CONSUMER_GROUP_ID           | consumer group id of the straming app's consumer      | 
| INPUT_TOPIC                 | input topic name where the {'ts','uid'} messages are produced      | 
| SEC_METRICS_OUTPUT_TOPIC    | output topic name with per-second metrics messages      |
| MIN_METRICS_OUTPUT_TOPIC    | output topic name with per-minute metrics messages      |
| OUTPUT_STREAM               | output topic name with frames received and produced by streaming-app     |
| PRINT_INTERVAL              | a number of messages after which to print execution metrics      |


## Setup & Running Steps (on Linux OS):
1. Create required folders:
   ```
   $ cd simple-streaming-app && mkdir logging && mkdir data
   ```
2. Upload input stream.jsonl to data folder;
3. Start required services:
   ```
   $ docker-compose up -d
   ```
4. Create and activate virtual environment:
   ```
   $ pipenv install
   $ pipenv shell
   ```
5. Run application:
   ```
   $ python app.py [-h] [--verbose] [--no-verbose]

   <!-- optional arguments:
   -h, --help    show this help message and exit
   --verbose
   --no-verbose -->
   ```

## Execution time:
|         | milion messages processed exec. time (minutes)           | avg. message processing exec. time (miliseconds)  |
| ------------- |:-------------:| -----:|
| **publish each frame to output topic**      | 14.75 | 0.8 |
| **publish only per-second and -minute metrics**      | 2.3      |   4.05e-03 |


## Improvements and TO-DOs:
* Dockerize simple-streaming-app:
  * create a Dockerfile to build an image and write container configuration in *docker-compose.yml*
* Error recovery...;
* Scaling: (this is just a *work in progress* idea :) ) Use *'ts'* field to parition data in discrete time intervals (eg. 15 minutes, 4 partitions would cover 1 hour) and have a number of consumers in the same consumer group to consume messages from each partition (4 consumers to consume messages from 4 partitions).
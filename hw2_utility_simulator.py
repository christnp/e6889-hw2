#!/usr/bin/env python2

# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Modifications copyright (C) 2019 Nicholas Christman
# Columbia University, 2019
# ELEN-E6889: Homework 2
# 
# Author:   Nick Christman, nc2677
# Date:     2019/03/12
# Program:  hw2_utility_simulator.py
# SDK(s):   Google Cloud Pub/Sub API


'''This file originates from the send_sensor_data.py script created by Google
as an example sensor simulator for the Pub/Sub dataflow application. The file
has been modified to act as a simple simulator for different utilities that a  
building would have during the day.

 References:
 1. https://codelabs.developers.google.com/codelabs/cpb104-pubsub/
 2. https://catalog.data.gov/dataset/residential-loads-may-2-2016-fc120
'''

# Begin imports
import csv
import argparse
import logging
import sys
import os
import re
import ipaddress
import socket
from datetime import datetime
#import datetime
import time
import gzip
from google.cloud import pubsub

TIME_FORMAT = '%m/%d/%Y %H:%M:%S.%f %p'
PROJECT = 'elen-e6889'
TOPIC_OUT = 'util-simulator'
#INPUT = 'Export_SPL_House2_050216_Data_test.csv.gz'
INPUT = 'Export_SPL_House2_050216_Data.csv.gz'
SAMP_RATE = 1

def publish(publisher, topic, events):
    numobs = len(events)
    if numobs > 0:
        logging.info('Publishing {0} events from {1}'.format(numobs, events[0]))
        for event_data in events:
            publisher.publish(topic,event_data)

def get_timestamp(line):
    ## convert from bytes to str
    line = line.decode('utf-8')

    # look at first field of row
    timestamp = line.split(',')[0]
    return datetime.strptime(timestamp, TIME_FORMAT)

def put_timestamp(line,hdr):
    ## convert from bytes to str
    line = line.decode('utf-8')
    hdr = hdr.decode('utf-8')

    # split header and line CSVs into list
    hdr_line = hdr.split(',')
    mod_line = line.split(',')
    
    # assume timestamp is at first field of row
    # mod_line[0] = time.time()
    new_line = zip(hdr_line,mod_line)
    # the_line = [list(tup)+[time.time()] for tup in new_line]
    
    # print([time.time()]*len(mod_line))
    # sys.exit()
    logging.info('put_timestamp: {}'.format(new_line))

    new_line = ','.join(map(str, mod_line))
    return new_line


def simulate(topic, ifp, header, firstObsTime, programStart, sampRate):
    # sleep computation
    # def compute_sleep_secs(obs_time):
    #     time_elapsed = (datetime.utcnow() - programStart).seconds
    #     sim_time_elapsed = (obs_time - firstObsTime).seconds / sampRate
    #     to_sleep_secs = sim_time_elapsed - time_elapsed
    #     return to_sleep_secs

    topublish = list() 
    to_sleep_secs = 1/sampRate

    header = header.split(',')
    ema = [0]*len(header)
    #N = # of time periods or size of windows
    N = 4 # Window is 1 minute
    alpha = .9#2/float(1+N)
    #with open('truth_data.csv', 'w') as testData:
    with open('test_data.csv', 'w') as testData,open('ema_truth_data.csv','w') as truthData:
        test_csv = csv.writer(testData)
        truth_csv = csv.writer(truthData)
        
        for count,line in enumerate(ifp):
            line = line.split(',')
            count = 0            

            sleep_time = float(to_sleep_secs)/(float(len(line)))

            # iterate through each element of the line, remove units
            data_out = list() # empty list for output
            for idx,data in enumerate(line):
                # we only want to pass floating point data, no strings (i.e., "W")
                tmp_header,x,y = (header[idx].lstrip()).partition(' ')
                try:
                    float(data)
                    #data_out.append((tmp_header,data,time.time()))
                    data_out = [(tmp_header,data,time.time())]

                    # truth data, moving average
                    ema[count] = float(data)*alpha + ema[count-1]*(1-alpha)
                    
                    # Writes to output_data.csv file for DirectRunner 
                    test_csv.writerows(data_out)

                    # publish the data
                    logging.debug("data out: {} \n".format(data_out))
                    #publish(publisher, topic, data_out)

                    logging.info("Publishing... {} {}".format(count,data_out[count]))
                    count += 1
                    time.sleep(sleep_time) # simluate realistic sample times

                except:
                    logging.info("Failed: data = {} \n".format(data_out))

                    pass
                publish(publisher, topic, data_out)
                
            # keep track of moving average
            logging.debug("exponential moving average: \n {}".format(ema))
            ema.insert(0,datetime.now().strftime("%H:%M:%S"))
            truth_csv.writerows([ema])

            logging.info('\n[{}] Sent full data sample\n'.format(datetime.now().strftime("%H:%M:%S")))

    # left-over records; notify again
    publish(publisher, topic, data_out)
    data_out = list()
    testData.close()

def peek_timestamp(ifp):
    # peek ahead to next line, get timestamp and go back
    pos = ifp.tell()
    line = ifp.readline()
    ifp.seek(pos)
    return get_timestamp(line)

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i',
                        dest='input',
                        default=INPUT,
                        help='Path of input file to process.If only name is ' \
                            'provided then local directory selected. Default:' \
                            '{}'.format(INPUT))
    parser.add_argument('--project', '-p',
                        dest='project',
                        default=PROJECT,
                        help='Google project used for Pub/Sub and Dataflow. ' \
                            'This project mus be configured prior to executing ' \
                            'the pipeline. Default: \'{}\''.format(PROJECT))
    parser.add_argument('--output', '-o',
                        dest='topic_out',
                        default=TOPIC_OUT,
                        help='Output topic for publishing. The output topic ' \
                            'must be configured prior to executing the ' \
                            'pipleine. Default: \'{}\''.format(TOPIC_OUT))
    parser.add_argument('--rate', '-r',
                        dest='samp_rate',
                        type=float,
                        default=SAMP_RATE,
                        help='Number of simulated samples per second. This is ' \
                            'how fast the data will be read per line ' \
                            '(lines/sec). Default: \'{}\''.format(SAMP_RATE))
                        

    args = parser.parse_args()
    sim_in = args.input
    project = args.project
    topic_out = args.topic_out
    samp_rate = float(args.samp_rate)

    # # create Pub/Sub notification topic
    # logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    # publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(project,topic_out)
    # try:
    #     publisher.get_topic(event_type)
    #     logging.info('Reusing pub/sub topic {}'.format(topic_out))
    # except:
    #     publisher.create_topic(event_type)
    #     logging.info('Creating pub/sub topic {}'.format(topic_out))

    # notify about each line in the input file
    programStartTime = datetime.utcnow() 
    with gzip.open(sim_in, 'rb') as ifp:
        header = ifp.readline()  # grab header
        firstObsTime = peek_timestamp(ifp)
        #firstObsTime = programStartTime + 1
        logging.info('Sending utility data from {}'.format(firstObsTime))
        simulate(event_type, ifp, header, firstObsTime, programStartTime, samp_rate)


if __name__ == '__main__':
    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()

    # execute the main script    
    run()  

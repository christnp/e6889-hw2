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

# Local configuraiton class
from config import *

# Begin imports
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

def publish(publisher, topic, events):
   numobs = len(events)
   if numobs > 0:
       logging.info('Publishing {0} events from {1}'.format(numobs, get_timestamp(events[0])))
       for event_data in events:
         publisher.publish(topic,event_data)

def get_timestamp(line):
   ## convert from bytes to str
   line = line.decode('utf-8')

   # look at first field of row
   timestamp = line.split(',')[0]
   return datetime.strptime(timestamp, TIME_FORMAT)

def simulate(topic, ifp, firstObsTime, programStart, speedFactor):
   # sleep computation
   def compute_sleep_secs(obs_time):
        time_elapsed = (datetime.utcnow() - programStart).seconds
        sim_time_elapsed = (obs_time - firstObsTime).seconds / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs

   topublish = list() 

   for line in ifp:
       event_data = line   # entire line of input CSV is the message
       obs_time = get_timestamp(line) # from first column

       # how much time should we sleep?
       if compute_sleep_secs(obs_time) > 1:
          # notify the accumulated topublish
          publish(publisher, topic, topublish) # notify accumulated messages
          topublish = list() # empty out list

          # recompute sleep, since notification takes a while
          to_sleep_secs = compute_sleep_secs(obs_time)
          if to_sleep_secs > 0:
             logging.info('Sleeping {} seconds'.format(to_sleep_secs))
             time.sleep(to_sleep_secs)
       topublish.append(event_data)

   # left-over records; notify again
   publish(publisher, topic, topublish)

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
                          'provided then local directory selected.')
    parser.add_argument('--project', '-p',
                        dest='project',
                        default=PROJECT,
                        help='Google project used for Pub/Sub and Dataflow. ' \
                          'This project mus be configured prior to executing ' \
                          'the pipeline. Default: \'elen-e6889\'')
    parser.add_argument('--output', '-o',
                        dest='topic_out',
                        default=TOPIC_OUT,
                         help='Output topic for publishing. The output topic ' \
                          'must be configured prior to executing the ' \
                          'pipleine. Default: \'util-output\'')
                        

    args = parser.parse_args()
    sim_in = args.input
    project = args.project
    topic_out = args.topic_out

    # # create Pub/Sub notification topic
    # logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    # publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(project,topic_out)
    try:
        publisher.get_topic(event_type)
        logging.info('Reusing pub/sub topic {}'.format(topic_out))
    except:
        publisher.create_topic(event_type)
        logging.info('Creating pub/sub topic {}'.format(topic_out))

    # notify about each line in the input file
    programStartTime = datetime.utcnow() 
    with gzip.open(sim_in, 'rb') as ifp:
        header = ifp.readline()  # skip header
        firstObsTime = peek_timestamp(ifp)
        logging.info('Sending utility data from {}'.format(firstObsTime))
        simulate(event_type, ifp, firstObsTime, programStartTime, 1)


if __name__ == '__main__':
    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()

    # execute the main script    
    run()  

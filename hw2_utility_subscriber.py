
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 2
# 
# Author:   Nick Christman, nc2677
# Date:     2019/03/12
# Program:  hw2_utility_subscriber.py
# SDK(s):   Google Cloud Pub/Sub API


'''This file is designed to subscibe to an existing Google Pub/Sub topic. A
subscription is created

 References:
 1. https://codelabs.developers.google.com/codelabs/cpb104-pubsub/
'''

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

# constants
TIME_FORMAT = '%m/%d/%Y %H:%M:%S.%f %p'
PROJECT = 'elen-e6889'
TOPIC_IN = 'util-output'
SUBSCRIPTION = 'util-sub-out'
OUTPUT = 'Output.txt'

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-p',
                        dest='project',
                        default=PROJECT,
                        help='Google project used for Pub/Sub and Dataflow. ' \
                          'This project mus be configured prior to executing ' \
                          'the pipeline. Default: \'elen-e6889\'')
    parser.add_argument('--input', '-i',
                        dest='topic_in',
                        default=TOPIC_IN,
                        help='Input topic for subscribing. The input topic ' \
                          'must be configured prior to executing the ' \
                          'pipeline. If using the included simulator, the '\
                          'topic will be created if it does not exist. '\
                          'Default: \'util-sub\'')
    parser.add_argument('--output', '-o',
                        dest='output',
                        default=OUTPUT,
                        help='Path of output/results file. If only name is ' \
                          'provided then local directory selected.')
    parser.add_argument('--subscription', '-s',
                        dest='subscription',
                        default=SUBSCRIPTION,
                        help='Subscription name. The input topic ' \
                          'must be configured prior to executing the ' \
                          'pipeline. If using the included simulator, the '\
                          'topic will be created if it does not exist. '\
                          'Default: \'util-simulator\'')                   
                        

    args = parser.parse_args()
    results_out = args.output
    project = args.project
    topic_in = args.topic_in
    subscription = args.subscription

    subscriber = pubsub.SubscriberClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project,
        topic=topic_in,
    )
    subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
        project_id=project,
        sub=subscription,
    )

    try:
        subscriber.create_subscription(
            name=subscription_name, topic=topic_name)
    except:
        print("Subscription \'{}\' already exists\n!".format(subscription_name))

    def callback(message):
        current = message.data
        print('Received message: {}'.format(message))
        # write line to csv
        # if csv is X bytes, close it and create new
        previous = current
        message.ack()

    future = subscriber.subscribe(subscription_name, callback)

    #Blocks the thread while messages are coming in through the stream. Any
    # exceptions that crop up on the thread will be set on the future.
    try:
        # When timeout is unspecified, the result method waits indefinitely.
        future.result(timeout=30)
    except Exception as e:
        print(
            'Listening for messages on {} threw an Exception: {}.'.format(
                subscription_name, e))  
    

if __name__ == '__main__':
    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()

    # execute the main script    
    run()  
# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 2
# 
# Author:   Nick Christman, nc2677
# Date:     2019/03/12
# Program:  hw2_original.py
# SDK(s):   Apache Beam

'''This file is the unoptimized, original streaming dataflow for Homework 2.

Assumptions: Python streaming pipeline execution is experimentally available 
(with some limitations) starting with Beam SDK version 2.5.0.

 References:
 1.  https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
'''
from __future__ import absolute_import
from __future__ import division

import argparse
import logging
import sys
import os
import time

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from google.cloud import pubsub

TIME_FORMAT = '%m/%d/%Y %H:%M:%S.%f %p'
PROJECT = 'elen-e6889'
TOPIC_IN = 'util-simulator'
TOPIC_OUT = 'util-output'
SUBSCRIPTION = 'util-sub'
DFLOW_TEMP = 'gs://e6889-bucket/tmp/'
STAGE = 'gs://e6889-bucket/stage/'
TARGET_UTILITY = 'DP2_WholeHouse_Power_Val'
RUNNER = 'DataflowRunner'

######################################################
# CUSTOM PTRANSFORMS
######################################################
# PTransform: Extracts the target value and averages (eper window)
class ExtractAndAverageTarget(beam.PTransform):
  """A transform to extract key/power information and average the power.
  The constructor argument `target` determines what target info is
  extracted. The 

  Reference: Apache Beam hourly_team_score.py example
  """
  def __init__(self, target,optimized):
    super(ExtractAndAverageTarget, self).__init__()
    self.target = target
    self.optimized = optimized

  def expand(self, pcoll):

    logging.info('ExtractAndAverageTarget(): averaging {} \n'.format(self.target)) 
    if self.optimized:
      # Reordering optimizaiton applied
      output = (pcoll | beam.Filter(lambda x: x[0] == self.target)
                      | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
    else:
      output = (pcoll | beam.CombinePerKey(beam.combiners.MeanCombineFn())
                      | beam.Filter(lambda x: x[0] == self.target))                    

    return output
            

# END CUSTOM PTRANSFORMS
######################################################


######################################################
# PIPELINE
######################################################
def run(argv=None):
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
                          'Default: \'util-simulator\'')
    parser.add_argument('--output', '-o',
                        dest='topic_out',
                        default=TOPIC_OUT,
                        help='Output topic for publishing. The output topic ' \
                          'must be configured prior to executing the ' \
                          'pipleine. Default: \'util-output\'')
    parser.add_argument('--temp', '-t',
                        dest='dflow_temp',
                        default=DFLOW_TEMP,
                        help='Google dataflow temporary storage. Defauls ' \
                              '\'gs://e6889-bucket/tmp/\'')
    parser.add_argument('--stage', '-s',
                        dest='stage',
                        default=STAGE,
                        help='Google dataflow staging arge. Default: ' \
                              '\'gs://e6889-bucket/stage/\'')
    parser.add_argument('--target', '-g',
                        dest='target',
                        default=TARGET_UTILITY,
                        help='Target utility to be averaged. Default: ' \
                              '\'DP2_WholeHouse_Power_Val\'')
    parser.add_argument('--optimized', '-m',
                        dest='optimized',
                        default=False,
                        help='Run the optimized dataflow (True or False). '\
                        'Default: \'False\'')
    parser.add_argument('--runner', '-r',
                        dest='runner',
                        default=RUNNER,
                        help='Target utility to be averaged. Default: ' \
                              '\'DP2_WholeHouse_Power_Val\'')
    args = parser.parse_args()
    project = args.project
    topic_in = args.topic_in
    topic_out = args.topic_out
    dflow_temp = args.dflow_temp
    stage = args.stage
    target = args.target
    optimized = args.optimized
    runner = args.runner

    # static
    sub_in = 'util-sub-sim'
    sub_out = 'util-sub-out'

   
    # Start Beam Pipeline
    pipeline_options = PipelineOptions()
    #pipeline_options.view_as(SetupOptions).requirements_file = 'requirements.txt'
    pipeline_options.view_as(SetupOptions).save_main_session = True # global session to workers
    pipeline_options.view_as(StandardOptions).runner = str(runner)
    if runner == 'DataflowRunner':
      pipeline_options.view_as(StandardOptions).streaming = True
      pipeline_options.view_as(GoogleCloudOptions).project = project
      pipeline_options.view_as(GoogleCloudOptions).temp_location = dflow_temp
      pipeline_options.view_as(GoogleCloudOptions).staging_location = stage
      
      # pubsub Config
      subscriber = pubsub.SubscriberClient()
      # topics and subscriptions
      topic_in_path = 'projects/{:s}/topics/{:s}'.format(project,topic_in)
      sub_in_path = 'projects/{:s}/subscriptions/{:s}'.format(project,sub_in)
      try:
        subscriber.create_subscription(
              name=sub_in_path, topic=topic_in_path)
      except:
          print("Subscription \'{}\' already exists\n!".format(sub_in_path))
      
      # pub/sub output topics and subscriptions
      topic_out_path = 'projects/{:s}/topics/{:s}'.format(project,topic_out)
      sub_out_path = 'projects/{:s}/subscriptions/{:s}'.format(project,sub_out)
      try:
        subscriber.create_subscription(
              name=sub_out_path, topic=topic_out_path)
      except:
          print("Subscription \'{}\' already exists\n!".format(sub_out_path))

    # Define pipline
    p = beam.Pipeline(options=pipeline_options)

    if runner == 'Direct':  
      row = (p | 'ReadData' >> beam.io.ReadFromText('test_data.csv'))  
    #row = (p | 'ReadData' >> beam.io.ReadFromText('gs://e6889-bucket/data/Export_SPL_House2_050216_Data_test.csv'))
    else:  
      row = (p | 'GetData' >>  beam.io.ReadFromPubSub(  
                                   #topic=topic_in_path,
                                   subscription=sub_in_path)
                                   .with_output_types(bytes))

    pane = (row | 'ParseData' >> beam.ParDo(ParseDataFn())
                | 'AddTimestamp' >> beam.ParDo(AddTimestampFn()))
                # | 'AddTimestsamp' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem,elem.pop('Timestamp',None)))
                #| 'Window' >> beam.WindowInto(FixedWindows(2 ,0)))#, #size=10,offset=0
                                # trigger=AfterWatermark(
                                #             late=AfterProcessingTime(30)), # allow for late data up to 30 seconds after window
                                # accumulation_mode=AccumulationMode.DISCARDING))

    def filterTarget(element, target):
      logging.info(" Element: {}\n Target: {}".format(element[0],target))
      logging.info("Equal? {}".format(element[0]==target))


      return element  
    output = (pane  | 'TargetAvg' >> ExtractAndAverageTarget(target,optimized)
                    #| beam.CombinePerKey(beam.combiners.MeanCombineFn())
                    #| beam.Filter(lambda x: x[0]==target)
                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn()))

    if runner == 'Direct':
      output | beam.io.WriteToText('output.csv')
    else:
      #output | beam.io.WriteToText('gs://e6889-bucket/results/hw2/output.csv')
      output | beam.io.WriteToPubSub( 
                   topic=topic_out_path)#,
                   #subscription=sub_out_path)


    # Execute the Pipline
    result = p.run()
    result.wait_until_finish()

# END PIPELINE
######################################################


######################################################
# CUSTOM PARDO FUNCTIONS
######################################################

# Transform: parses PCollection elements  
class ParseDataFn(beam.DoFn):
  """Parses the raw whole house utility event info into a Python dictionary.
    Each event line has the format:
      readable_datetime,value_1,units_1,value_2,units_2,...,Test_T/F
    e.g.:
      05/02/2016 08:33:53.56 AM, 33.863, W, -27.1889, W, ..., TRUE
    All units are consitently in watts (W) and will be removed. Likewise, the
    last column (Test) is not necessary and will be removed.    
  """
  def __init__(self): 
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  # main process
  def process(self,element):
    element.encode('utf-8')
    elements = element.split(',')
    # assume CSV as data input format    
    key = elements[0]
    try:
      value = float(elements[1])
    except:
      value = 0
      self.num_parse_errors.inc()

      logging.critical("ParseDataFn(): value parse error for \'{}\'".format(key))
    try:
      unix_ts = float(elements[2]) #element.pop('Timestamp',None) 
    except:
      unix_ts = 0
      self.num_parse_errors.inc()
      logging.critical("ParseDataFn(): timestamp parse error for \'{}\'".format(key))
     
    logging.debug("ParseDataFn(): ({},{},{})".format(key,value,unix_ts))

    # new_element = dict((key,value))
      #yield zip(self.keys,values) # list of tuples((a,1),(b,2),(c,3))
    yield [key,value,unix_ts]
    
# ParDo Transform: adds timestamp to element
# Ref: Beam Programming Guide
class AddTimestampFn(beam.DoFn):
  def process(self, element):
    # pop the timestamp off the dictionary (NOTE: this removes it from element)
    # PCollecton format: [key,value,unix_ts]
    key_value = (element[0],element[1])
    unix_ts = element[2]
    logging.debug('AddTimestampFn(): {} {}\n'.format(unix_ts,key_value)) 
      
    yield beam.window.TimestampedValue(key_value,unix_ts)

# Transform: format the output
class FormatOutputFn(beam.DoFn):
  def process(self,rawOutput,window=beam.DoFn.WindowParam):
    # define output format
    
    start = 0#window.start.to_utc_datetime().strftime(TIME_FORMAT)
    end = 1#window.end.to_utc_datetime().strftime(TIME_FORMAT)
    # Format as CSV: "AVG(target),average_power,start of period,end of period"
    formatApply = "AVG({}), {}, {}, {}"
    formattedOutput = formatApply.format(rawOutput[0],rawOutput[1],start,end)
   
    logging.info('FormatOutputFn() {}\n'.format(formattedOutput))
    return [formattedOutput]

# END CUSTOM PARDO FUNCTIONS
######################################################


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
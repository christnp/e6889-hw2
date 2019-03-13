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
  The constructor argument `field` determines what target info is
  extracted.

  Reference: Apache Beam hourly_team_score.py example
  """
  def __init__(self, field):
    super(ExtractAndAverageTarget, self).__init__()
    self.field = field

  def expand(self, pcoll):

    def tmp(element,target):
      logging.info('ExtractAndAverageTarget(): {} \n'.format((target,float(element[target])))) 
      #logging.info('ExtractAndAverageTarget(): {} \n'.format(element.items()))
      return (target,element[target])
      #return element.items()


    return (pcoll
            #| beam.Map(lambda elem: (self.field, elem[self.field]))
            | beam.Map(tmp,self.field)
            | beam.CombinePerKey(beam.combiners.MeanCombineFn()))

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
    runner = args.runner

    # static
    sub_in = 'util-sub-sim'
    sub_out = 'util-sub-out'

   
    # Start Beam Pipeline
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).requirements_file = 'requirements.txt'
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
      row = (p | 'ReadData' >> beam.io.ReadFromText('Export_SPL_House2_050216_Data.csv'))  
    #row = (p | 'ReadData' >> beam.io.ReadFromText('gs://e6889-bucket/data/Export_SPL_House2_050216_Data_test.csv'))
    else:  
      row = (p | 'GetData' >>  beam.io.ReadFromPubSub(  
                                   #topic=topic_in_path,
                                   subscription=sub_in_path)
                                   .with_output_types(bytes))

    pane = (row | 'ParseData' >> beam.ParDo(ParseDataFn())
                | 'ParseTimestamp' >> beam.ParDo(ParseTimestampFn()) # Optimization 1: merge with ParseData
                | 'AddTimestamp' >> beam.ParDo(AddTimestampFn())
                # | 'AddTimestsamp' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem,elem.pop('Timestamp',None)))
                | 'Window' >> beam.WindowInto(FixedWindows(10 ,0), #size=10,offset=0
                                trigger=AfterWatermark(
                                            late=AfterProcessingTime(30)), # allow for late data up to 30 seconds after window
                                accumulation_mode=AccumulationMode.DISCARDING))

#                    | 'CombineAsList' >> beam.CombineGlobally(
#                                              beam.combiners.ToListCombineFn()).without_defaults()
    output = (pane  #| 'FilterTarget' >> beam.Map(lambda x: (target,x[target])) #Optimization 2: filter in ParseData
#                    | 'TargetAvg' >> beam.CombinePerKey(beam.combiners.MeanCombineFn()) # average value per window pane
                    | 'TargetAvg' >> ExtractAndAverageTarget(target)
                    #| 'TargetFilter' >> beam.Map(lambda elem: (target, elem[target]))
                    #| 'TargetAvg' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
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

    # dictionary key data structure
    self.keys = ["DP2_WholeHouse_Power_Val", 
        "DP2_WholeHouse_VAR_Val", "DP2_Condenser_Power_Val", 
        "DP2_Condenser_VAR_Val", "DP2_AirHandler_Power_Val", 
        "DP2_AirHandler_VAR_Val", "DP2_WaterHeater_Power_Val", 
        "DP2_Dryer_Power_Val", "DP2_Range_Power_Val", 
        "DP2_Refrigerator_Power_Val", "DP2_Washer_Power_Val", 
        "DP2_Dishwasher_Power_Val", "DP2_Lights_Power_Val", 
        "DP2_NRecept_Power_Val", "DP2_CounterRecpt_Power_Val", 
        "DP2_WDRecpt_Power_Val", "Timestamp"]

  # main process
  def process(self,element):
    # assume CSV as data input format
    try:
      #elements = list(csv.reader([element]))[0]
      elements = element.split(',')
      values = [float(i) for i in elements[1:-1:2]] #remove unit and test columns
      values.append(elements[0]) # add timestamp to the end
      logging.info('Parsed values: \'%s\'', values)
      # Optimization 1: "merge"      
      yield dict(zip(self.keys,values)) # save as dictionary
      #yield zip(self.keys,values) # list of tuples((a,1),(b,2),(c,3))

    except:
      self.num_parse_errors.inc()
      logging.error('Parse error on \'%s\'', element)
    
class ParseTimestampFn(beam.DoFn):
  def process(self, element):
    from datetime import datetime 

    logging.info('ConvertTimestampFn(): Timestamp {}\n'.format(element["Timestamp"]))
    dt_obj = datetime.strptime(element["Timestamp"], TIME_FORMAT) 
    unix_ts = time.mktime(dt_obj.timetuple())
    element["Timestamp"] = float(unix_ts)
    yield element

# ParDo Transform: adds timestamp to element
# Ref: Beam Programming Guide
class AddTimestampFn(beam.DoFn):
  def process(self, element):
    # pop the timestamp off the dictionary (NOTE: this removes it from element)
    logging.info('AddTimestampFn(): {}\n'.format(element))    
    unix_ts = element.pop('Timestamp',None) 
    logging.info('AddTimestampFn(): {} -> {}\n'.format(unix_ts,element))  
    yield beam.window.TimestampedValue(element, unix_ts)

# Transform: format the output as 'IP : size'
class FormatOutputFn(beam.DoFn):
  def process(self,rawOutput,window=beam.DoFn.WindowParam):
    # define output format
    
    start = window.start.to_utc_datetime().strftime(TIME_FORMAT)
    end = window.end.to_utc_datetime().strftime(TIME_FORMAT)
    # Format as CSV: "AVG(target),average_power,start of period,end of period"
    formatApply = "AVG({:s}), {:f}, {:s}, {:s}"
    formattedOutput = formatApply.format(rawOutput[0],rawOutput[1],start,end)# Transform: format the output as 'IP : size'
   
    logging.info('FormatOutputFn() {}\n'.format(formattedOutput))
    return [formattedOutput]

# END CUSTOM PARDO FUNCTIONS
######################################################


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
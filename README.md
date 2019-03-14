# Introduction
There are two primary Python 2.7 scripts included in this assignment: 
hw2_optimizations.py and hw2_utility_simulator.py. By default and assuming the 
Google Cloud enviroment is set up and configured properly (see attached report and Notes
provided below), both progams can be ran without any command-line options. 
However, if customization is necessary, please refer to the usage below.

# hw2_optimizations.py Usage

usage: hw2_optimizations.py [-h] [--project PROJECT] [--input TOPIC_IN]
                          [--output TOPIC_OUT] [--temp DFLOW_TEMP]
                          [--stage STAGE] [--target TARGET]
                          [--optimized OPTIMIZED] [--runner RUNNER]

optional arguments:
  -h, --help            show this help message and exit
  --project PROJECT, -p PROJECT
                        Google project used for Pub/Sub and Dataflow. This
                        project mus be configured prior to executing the
                        pipeline. Default: 'elen-e6889'
  --input TOPIC_IN, -i TOPIC_IN
                        Input topic for subscribing. The input topic must be
                        configured prior to executing the pipeline. If using
                        the included simulator, the topic will be created if
                        it does not exist. Default: 'util-simulator'
  --output TOPIC_OUT, -o TOPIC_OUT
                        Output topic for publishing. The output topic must be
                        configured prior to executing the pipleine. Default:
                        'util-output'
  --temp DFLOW_TEMP, -t DFLOW_TEMP
                        Google dataflow temporary storage. Defauls
                        'gs://e6889-bucket/tmp/'
  --stage STAGE, -s STAGE
                        Google dataflow staging arge. Default:
                        'gs://e6889-bucket/stage/'
  --target TARGET, -g TARGET
                        Target utility to be averaged. Default:
                        'DP2_WholeHouse_Power_Val'
  --optimized OPTIMIZED, -m OPTIMIZED
                        Run the optimized dataflow (True or False). Default:
                        'False'
  --runner RUNNER, -r RUNNER
                        Target utility to be averaged. Default:
                        'DP2_WholeHouse_Power_Val'


# hw2_utility_simulator.py Usage
usage: hw2_utility_simulator.py [-h] [--input INPUT] [--project PROJECT]
                                [--output TOPIC_OUT] [--rate SAMP_RATE]
                                [--csv TEST_CSV]

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT, -i INPUT
                        Path of input file to process.If only name is provided
                        then local directory selected.
                        Default:Export_SPL_House2_050216_Data.csv.gz
  --project PROJECT, -p PROJECT
                        Google project used for Pub/Sub and Dataflow. This
                        project mus be configured prior to executing the
                        pipeline. Default: 'elen-e6889'
  --output TOPIC_OUT, -o TOPIC_OUT
                        Output topic for publishing. The output topic must be
                        configured prior to executing the pipleine. Default:
                        'util-simulator'
  --rate SAMP_RATE, -r SAMP_RATE
                        Number of simulated samples per second. This is how
                        fast the data will be read per line (lines/sec).
                        Default: '0.5'
  --csv TEST_CSV, -c TEST_CSV
                        Number of simulated samples per second. This is how
                        fast the data will be read per line (lines/sec).
                        Default: 'test_data.csv'

# Notes

1. Make sure to install Python Dependicies for dataflow:
https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/

https://www.hopkinsmedicine.org/healthlibrary/conditions/cardiovascular_diseases/vital_signs_body_temperature_pulse_rate_respiration_rate_blood_pressure_85,p00866

https://catalog.data.gov/dataset/residential-loads-may-2-2016-fc120


Change simulator so it replaces old timestamp with new... or converts timestamp to UNIX
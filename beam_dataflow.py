import apache_beam as beam
import argparse
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.options.pipeline_options import PipelineOptions

def printing(word):
    print(word)

def splitting(line):
    return line.split(',')

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--rundate',required=False,help='Run_Date')
    parser.add_argument('--project',required=True,help='GCP Project Name')
    parser.add_argument('--bucket',required=True,help='bucketname')
    parser.add_argument('--runner',required=True,help='target env')
    parser.add_argument('--job_name',required=False,help='jobname',default='firstjob')
    parser.add_argument('--region',required=False,help='region',default='us-central1')
    parser.add_argument('--save_main_session',required=False)


    known_args,pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    input = 'gs://{0}/source_data/dept_data_{1}.csv'.format(known_args.bucket,known_args.rundate)

    argv = [
            '--project={0}'.format(known_args.project),
            '--job_name={0}'.format(known_args.job_name),
            '--save_main_session',
            '--staging_location=gs://{0}/staging'.format(known_args.bucket),
            '--temp_location=gs://{0}/staging'.format(known_args.bucket),
            '--region={0}'.format(known_args.region),
            '--runner={0}'.format(known_args.runner)
            ]

    output = 'gs://{0}/output_data/df_out1_{1}'.format(known_args.bucket,known_args.rundate)
    #input = 'gs://{0}/source_data/*.java'.format(known_args.bucket)
    for value in pipeline_args:
        print(value)
    print(len(pipeline_args))
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            |beam.io.ReadFromText(input)
            |beam.FlatMap(lambda line: splitting(line))
            |dollars()
            |beam.io.WriteToText(output)
      #      |beam.Map(lambda word:printing(word))
        )

class dollars(beam.PTransform):
    def __init__(self):
        beam.PTransform.__init__(self)

    def fun1(self,word):
        print(word)
    
    def expand(self,words):
        return  (
                    words
                    |beam.Map(lambda modword: modword+'_$$$')
                    )

if __name__=='__main__':
    run()

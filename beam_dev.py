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
    parser.add_argument('--input',required=True,help='Input set of words')
    parser.add_argument('--output',required=False,help='Output location')
    known_args,pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            |beam.io.ReadFromText(known_args.input)
            |beam.FlatMap(lambda line: splitting(line))
            |dollars()
            |beam.Map(lambda word:printing(word))
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

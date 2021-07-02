import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class dollars(beam.PTransform):
    def __init__(self):
        beam.PTransform.__init__(self)

    def fun1(self,word):
        print(word)
    
    def test_ptransform(self):
        p1 = beam.Pipeline()

        testline = (
                    p1
                    |beam.Create(['hello','candle'])
                    |beam.Map(lambda modword: modword+'_$$$')
                    |beam.Map(lambda word: self.fun1(word))
                    )
        p1.run()

if __name__=='__main__':
    obj = dollars()
    obj.test_ptransform()

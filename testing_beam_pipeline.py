import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import beam_dev 
import apache_beam as beam

class dollars(unittest.TestCase):
    input = 'data/test_data.txt'
    
    def printing(self,word):
        print(word)

    def test1(self):
        print('test1')
        with TestPipeline() as p:
            words = p | beam.io.ReadFromText(self.input)
            result = words | beam_dev.dollars()
            assert_that(result,equal_to(['abc_$$$','123_$$$','xyz_$$$']))
            #result | beam.Map(lambda word: self.printing(word) )


if __name__=='__main__':
    unittest.main()

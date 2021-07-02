import apache_beam as beam
import re


def fsplit(line):
     return re.split('[ \t\n]+',line)



p1 = beam.Pipeline()

wordcount = (
                p1
                |beam.io.ReadFromText('data/data.txt')
                |beam.FlatMap(lambda line: fsplit(line))
                |beam.Map(lambda word: (word,1))
                |beam.CombinePerKey(sum)
                |beam.io.WriteToText('data/wordresult')
        )

p1.run()

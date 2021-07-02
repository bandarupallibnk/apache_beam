import apache_beam as beam

p1 = beam.Pipeline()

lines = (
            p1
#            | beam.Create(['hello!','beam','this is sampel data'])
            | beam.Create([('hello',1),('bye',2)])
            | beam.io.WriteToText('data/test101_data.txt')
        )
p1.run()

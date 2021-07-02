import apache_beam as beam

p1 = beam.Pipeline()

def SplitRow(line):
    return line.split(',')

def filtering(record):
    return record[3] == 'Accounts'

lines = (
            p1
            |beam.io.ReadFromText('data/dept_data.txt')
            |beam.Map(lambda line: SplitRow(line))
            |beam.Filter(lambda record: filtering(record))
            |beam.Map(lambda record:(record[1],1))
            |beam.CombinePerKey(sum)
            |beam.io.WriteToText('data/dept_out')
        )
p1.run()

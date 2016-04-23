from mrjob.job import MRJob

class FrequentVisitors(MRJob):

    def get_name(self, line):
    	fields = line.split(',')
    	## if header ..
    	return "{} {} {}".format(fields[1], fields[2], fields[0])

    def mapper(self, _, line):
        #print(type(line))
        name = self.get_name(line)
        yield None, 1

    def reducer(self, word, counts):
        yield None, sum(counts)

if __name__ == '__main__':
    FrequentVisitors.run()

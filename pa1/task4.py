from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol

class VisitorVisitee(MRJob):
    OUTPUT_PROTOCOL = protocol.TextValueProtocol

    def get_name(self, line):
    	fields = line.split(',')
        ## Ignore header
    	if fields[0] == "NAMELAST":
            return None
        name = " ".join(filter(lambda x: x != "", [fields[1], fields[2], fields[0]]))
    	return name.title()

    def mapper(self, _, line):
        name = self.get_name(line)
        if name != None:
            yield name, 1

    def combiner(self, name, counts):
        yield name, sum(counts)

    def reducer(self, name, counts):
        visits = sum(counts)
        if visits >= 10:
            yield None, name ##name, counts for testing
'''
    def final_reducer(self, _, counts):
        yield None, sum(counts)

    def final_mapper(self, _, entry):
        yield None, 1

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(mapper=self.final_mapper,
                   reducer=self.final_reducer)
        ]
'''
if __name__ == "__main__":
    VisitorVisitee.run()
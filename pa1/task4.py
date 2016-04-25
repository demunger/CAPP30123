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
        visitor = " ".join([fields[1], fields[0]]).title()

        ## Ignore single-name anomalies
        if fields[19] == "" or fields[20] == "":
            return None
        ## Ignore Visitor's Office anomaly
        if fields[19].lower() == "office" and fields[20].lower().strip() == "visitors":
            return None
        visitee = " ".join([fields[20], fields[19]]).title()

        return visitor, visitee


    def mapper(self, _, line):
        visitor, visitee = self.get_name(line)
        if None not in [visitor, visitee]:
            print("{} - {}".format(visitor, visitee))
            yield visitor, "visitor"
            yield visitee, "visitee"

    def reducer(self, name, counts):
        visitees = list(counts)
        if name in visitees:
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
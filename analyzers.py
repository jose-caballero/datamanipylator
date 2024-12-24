class Analyzer(object):
    pass


class AnalyzerIndexBy(Analyzer):
    analyzertype = "indexby"
    def indexby(self):
        """
        Implementation of an indexby() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the key under which this item will belong in the aggregated object
        """
        raise NotImplementedError


class AnalyzerFilter(Analyzer):
    analyzertype = "filter"
    def filter(self):
        """
        Implementation of a filter() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is a boolean indicating if the item should be kept or not
        """
        raise NotImplementedError


class AnalyzerMap(Analyzer):
    analyzertype = "map"
    def map(self):
        """
        Implementation of a map() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the modified item 
        """
        raise NotImplementedError


class AnalyzerReduce(Analyzer):
    analyzertype = "reduce"
    def __init__(self, init_value=None):
        self.init_value = init_value

    def initialvalue(self):
        return self.init_value

    def reduce(self):
        """
        Implementation of a reduce() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the aggregated result of analyzing the item and the previous value,
              which is being stored in a class attribute
        """
        raise NotImplementedError


class AnalyzerTransform(Analyzer):
    analyzertype = "transform"
    def transform(self):
        """
        Implementation of a transform() method:
            - the input is the entire list of data objects
            - the output is a new list of data object
        """
        raise NotImplementedError


class AnalyzerSort(Analyzer):
    analyzertype = "sort"
    def sort(self):
        """
        Implementation of a sort() method:
            - the input are 2 Data objects to compare
            - the output is -1, 0 or 1, based on the implemented sorting rules
        """
        raise NotImplementedError


class AnalyzerProcess(Analyzer):
    analyzertype = "process"
    def process(self):
        """
        Implementation of a process() method:
            - the input is the entire list of data objects
            - the output can be anything
        """
        raise NotImplementedError


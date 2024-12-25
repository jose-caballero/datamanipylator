class Analyzer(object):
    pass


class AnalyzerIndexBy(Analyzer):
    analyzertype = "indexby"
    def indexby(self, item):
        """
        Implementation of an indexby() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the key under which this item will belong in the aggregated object
        :param item: each element in Data.data
        :type item:
        :return: the key of the dictionary being created 
        :rtype: str
        """
        raise NotImplementedError


class AnalyzerFilter(Analyzer):
    analyzertype = "filter"
    def filter(self, item):
        """
        Implementation of a filter() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is a boolean indicating if the item should be kept or not
        :param item: each element in Data.data
        :type item:
        :return: whether the item should be kept or not
        :rtype: boolean 
        """
        raise NotImplementedError


class AnalyzerMap(Analyzer):
    analyzertype = "map"
    def map(self, item):
        """
        Implementation of a map() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the modified item 
        :param item: each element in Data.data
        :type item:
        :return: new value after applying a mapping function
        :rtype: 
        """
        raise NotImplementedError


class AnalyzerReduce(Analyzer):
    analyzertype = "reduce"
    def __init__(self, init_value=None):
        self.init_value = init_value

    def initialvalue(self):
        return self.init_value

    def reduce(self, item1, item2):
        """
        Implementation of a reduce() method:
            - the input is an individual item from the list of data objects being analyzed
            - the output is the aggregated result of analyzing the item and the previous value,
              which is being stored in a class attribute
        :param item1: each element in Data.data
        :type item1:
        :param item2: each element in Data.data
        :type item2:
        :return: the aggregated value
        :rtype: 
        """
        raise NotImplementedError


class AnalyzerTransform(Analyzer):
    analyzertype = "transform"
    def transform(self, l):
        """
        Implementation of a transform() method:
            - the input is the entire list of data objects
            - the output is a new list of data object
        :param l: an entire list of items 
        :type l: list
        :return: new list 
        :rtype: list 
        """
        raise NotImplementedError


class AnalyzerSort(Analyzer):
    analyzertype = "sort"
    def sort(self, item1, item2):
        """
        Implementation of a sort() method:
            - the input are 2 Data objects to compare
            - the output is -1, 0 or 1, based on the implemented sorting rules
        :param item1: each element in Data.data
        :type item1:
        :param item2: each element in Data.data
        :type item2:
        :return: -1, 0 or 1, depending on the sorting algorithm
        :rtype: int
        """
        raise NotImplementedError


class AnalyzerProcess(Analyzer):
    analyzertype = "process"
    def process(self):
        """
        Implementation of a process() method:
            - the input is the entire list of data objects
            - the output can be anything
        :param l: an entire list of items 
        :type l: list
        :return: anything
        :rtype: 
        """
        raise NotImplementedError


#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero.hep@gmail.com"


import datetime
import inspect
import logging
import logging.handlers
import threading
import time
import traceback
import os
import pwd
import sys

from  functools import reduce

from datamanipylator.exceptions import (
    IncorrectInputDataType,
    NotAnAnalyzer,
    IncorrectAnalyzer,
    MissingKeyException,
    AnalyzerFailure,
)
from datamanipylator.decorators import (
    validate_call,
    catch_exception,
)
from datamanipylator.analyzers import (
    AnalyzerIndexBy,
    AnalyzerFilter,
    AnalyzerMap,
    AnalyzerReduce,
    AnalyzerTransform,
    AnalyzerSort,
    AnalyzerProcess,
)

# =============================================================================
# Base classes and interfaces
# =============================================================================

class _Base(object):

    def __init__(self, data, timestamp=None):
        """ 
        :param data: the data to be recorded
        :param timestamp: the time when this object was created
        """ 
        self.log = logging.getLogger('info')
        self.log.addHandler(logging.NullHandler())

        msg ='Initializing object with input options: \
data={data}, timestamp={timestamp}'
        msg = msg.format(data=data,
                         timestamp=timestamp)
        self.log.debug(msg)

        self.data = data 

        if not timestamp:
            timestamp = int(time.time())
            msg = 'Setting timestamp to %s' %timestamp
            self.log.debug(msg)
        self.timestamp = timestamp

        self.log.debug('Object initialized')


    def get(self, *key_l):
        """
        returns the data hosted by the Info object in the 
        tree structure pointed by all keys
        The output is the data, either a dictionary or the original raw list 
        :param key_l list: list of keys for each nested dictionary
        :rtype data:
        """
        if len(key_l) == 0:
            return self.data
        else:
            key = key_l[0]
            if key not in self.data.keys():
                raise MissingKeyException(key)
            data = self.data[key]
            return data.get(*key_l[1:])


class _BaseDict(_Base):
    """
    adds an extra check for the input data
    """
    def __init__(self, data, timestamp=None):
        super(_BaseDict, self).__init__(data, timestamp)
        if type(self.data) is not dict:
            raise IncorrectInputDataType(dict)

    def getraw(self):
        out = {}
        for key, value in self.data.items():
            out[key] = value.getraw()
        return out

    def __getitem__(self, key):
        """
        returns the Info object pointed by the key
        :param key: the key in the higher level dictionary
        :rtype Data: 
        """
        if key not in self.data.keys():
            raise MissingKeyException(key)
        return self.data[key]

# extra get methods

class _GetRawBase:

    def getraw(self):
        return self.data


# interfaces 

class _AnalysisInterface:

    def indexby(self, analyzer):
        raise NotImplementedError

    def map(self, analyzer):
        raise NotImplementedError

    def filter(self, analyzer):
        raise NotImplementedError

    def reduce(self, analyzer):
        raise NotImplementedError

    def transform(self, analyzer):
        raise NotImplementedError

    def sort(self, analyzer):
        raise NotImplementedError

    def process(self, analyzer):
        raise NotImplementedError


# =============================================================================
# Info class
# =============================================================================

class Data(_Base, _AnalysisInterface, _GetRawBase):

    def __init__(self, data, timestamp=None):
        super(Data, self).__init__(data, timestamp)
        if type(self.data) is not list:
            msg = 'Input data %s is not a dict. Raising exception' %data
            self.log.error(msg)
            raise IncorrectInputDataType(list)


    def analyze(self, analyzer):
        """
        generic method that picks the right one 
        based on the type of analyzer
        :param analyzer: an Analyzer object 
        :rtype Data:
        """
        self.log.debug('Starting')
        if analyzer.analyzertype == 'indexby':
            return self.indexby(analyzer)
        elif analyzer.analyzertype == 'map':
            return self.map(analyzer)
        elif analyzer.analyzertype == 'filter':
            return self.filter(analyzer)
        elif analyzer.analyzertype == 'reduce':
            return self.reduce(analyzer)
        elif analyzer.analyzertype == 'transform':
            return self.transform(analyzer)
        elif analyzer.analyzertype == 'process':
            return self.process(analyzer)
        else:
            msg = 'Input object %s is not a valid analyzer. Raising exception.'
            self.log.error(msg)
            raise NotAnAnalyzer()


    def apply_algorithm(self, algorithm):
        """
        invoke all steps in an Algorithm object
        and returns the final output
        :param Algorithm algorithm: 
        :rtype Data:
        """
        return algorithm.analyze(self)

    # -------------------------------------------------------------------------
    # methods to manipulate the data
    # -------------------------------------------------------------------------

    @validate_call
    def indexby(self, analyzer):
        """
        groups the items recorded in self.data into a dictionary
        and creates a new Data object with it. 
           1. make a dictinary grouping items according to rules in analyzer
           2. convert that dictionary into a dictionary of Data objects
           3. make a new Data with that dictionary
        :param analyzer: an instance of AnalyzerIndexBy-type class 
                         implementing method indexby()
        :rtype Data:
        """
        self.log.debug('Starting with analyzer %s' %analyzer)

        new_data = self.__indexby(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __indexby(self, analyzer):
        # 1
        tmp_new_data = {}
        for item in self.data:
            key_l = analyzer.indexby(item)
            if key_l is not None:
                if not type(key_l) in  [tuple, list]:
                    # indexyby( ) may return a tuple, a list, or a single value
                    # in the last case, let's convert it into an interable
                    key_l = [key_l]
                for key in key_l:
                    if key not in tmp_new_data.keys():
                        tmp_new_data[key] = []
                    tmp_new_data[key].append(item)
        # 2
        new_data = {}
        for k, v in tmp_new_data.items():
            new_data[k] = Data(v, timestamp=self.timestamp)

        return new_data

    # -------------------------------------------------------------------------

    @validate_call
    def map(self, lambdamap):
        """
        modifies each item in self.data according to rules
        in analyzer
        :param lambdamap: an instance of AnalyzerMap-type class 
                          implementing method map()
                          or a function
        :rtype Data:
        """
        self.log.debug('Starting with lambda %s' %lambdamap)
        new_data = self.__map(lambdamap)
        new_info = Data(new_data, timestamp=self.timestamp)
        return new_info


    @catch_exception
    def __map(self, lambdamap):
        """
        call to python map() function
        """
        if isinstance(lambdamap, AnalyzerMap):
            return list(map(lambdamap.map, self.data))
        else:
            return list(map(lambdamap, self.data))

    # -------------------------------------------------------------------------

    @validate_call
    def filter(self, lambdafilter):
        """
        eliminates the items in self.data that do not pass
        the filter implemented in analyzer
        :param lambdafilter: an instance of AnalyzerFilter-type class 
                             implementing method filter()
                             or a function
        :rtype Data:
        """
        self.log.debug('Starting with lambda %s' %lambdafilter)
        new_data = self.__filter(lambdafilter)
        new_info = Data(new_data, timestamp=self.timestamp)
        return new_info


    @catch_exception
    def __filter(self, lambdafilter):
        """
        call to python filter() function
        """
        if isinstance(lambdafilter, AnalyzerFilter):
            return list(filter(lambdafilter.filter, self.data))
        else:
            return list(filter(lambdafilter, self.data))

    # -------------------------------------------------------------------------

    @validate_call
    def reduce(self, lambdareduce):
        """
        process the entire self.data at the raw level and accumulate values
        :param lambdareduce: an instance of AnalyzerReduce-type class 
                             implementing method reduce()
                             or a function
        :rtype Data: 
        """
        self.log.debug('Starting with lambda %s' %lambdareduce)
        new_data = self.__reduce(lambdareduce)
        new_info = _NonMutableData(new_data, 
                              timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __reduce(self, lambdareduce):
        """
        call to python reduce() function
        """
        if isinstance(lambdareduce, AnalyzerReduce):
            initialvalue = lambdareduce.initialvalue()
            if initialvalue is not None:
                return reduce(lambdareduce.reduce, self.data, initialvalue)
            else:
                return reduce(lambdareduce.reduce, self.data)
        else:
            return reduce(lambdareduce, self.data)
        
    # -------------------------------------------------------------------------

    @validate_call
    def transform(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an instance of AnalyzerTransform-type class 
                         implementing method transform()
        :rtype Data: 
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__transform(analyzer)
        new_info = Data(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __transform(self, analyzer):
        new_data = analyzer.transform(self.data)
        return new_data

    # -------------------------------------------------------------------------

    @validate_call
    def sort(self, lambdasort):
        """
        sorts the entire self.data at the raw level
        :param analyzer: an instance of AnalyzerSort-type class 
                         implementing method sort()
        :rtype Data: 
        """
        self.log.debug('Starting with sort lambda %s' %lambdasort)
        new_data = self.__sort(lambdasort)
        new_info = Data(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __sort(self, lambdasort):
        from functools import cmp_to_key
        sorted_data = sorted(self.data, key=cmp_to_key(lambdasort.sort))
        return sorted_data

    # -------------------------------------------------------------------------

    @validate_call
    def process(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an instance of AnalyzerProcess-type class 
                         implementing method process()
        :rtype Data: 
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__process(analyzer)
        new_info = _NonMutableData(new_data, timestamp=self.timestamp)
        return new_info
        
    @catch_exception
    def __process(self, analyzer):
        new_data = analyzer.process(self.data)
        return new_data

    def count(self):
        new_data = self.__count()
        new_info = _NonMutableData(new_data, timestamp=self.timestamp)
        return new_info
    
    def __count(self):
        return len(self.data)

# =============================================================================

class _DictData(_BaseDict, _AnalysisInterface):

    # -------------------------------------------------------------------------
    # methods to manipulate the data
    # -------------------------------------------------------------------------

    @validate_call
    def indexby(self, analyzer):
        new_data = {}
        for key, data in self.data.items():
            self.log.debug('calling indexby() for content in key %s'%key)
            new_data[key] = data.indexby(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info
    

    @validate_call
    def map(self, analyzer):
        new_data = {}
        for key, data in self.data.items():
            self.log.debug('calling map() for content in key %s'%key)
            new_data[key] = data.map(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def filter(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling filter() for content in key %s'%key)
            new_data[key] = data.filter(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def reduce(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling reduce() for content in key %s'%key)
            new_data[key] = data.reduce(analyzer)
        new_info = _NonMutableDictData(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def transform(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling transform() for content in key %s'%key)
            new_data[key] = data.transform(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def sort(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling sort() for content in key %s'%key)
            new_data[key] = data.sort(analyzer)
        new_info = _DictData(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def process(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling process() for content in key %s'%key)
            new_data[key] = data.process(analyzer)
        new_info = _NonMutableDictData(new_data, timestamp=self.timestamp)
        return new_info

    def count(self):
        new_data = {}
        for key, data in self.data.items():
            self.log.debug('calling process() for content in key %s'%key)
            new_data[key] = data.count()
        new_info = _NonMutableDictData(new_data, timestamp=self.timestamp)
        return new_info


class _NonMutableData(_Base, _GetRawBase):
    pass

class _NonMutableDictData(_BaseDict):
    pass



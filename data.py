#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero.hep@gmail.com"

"""
Code to store and manipulate data.

-------------------------------------------------------------------------------
                            class Data
-------------------------------------------------------------------------------

This is the only class implemented that is meant to be public.

The data stored by instances of class Data must be a list of items. 
These items can be anything, including objects. 
A typical example is data is a list of HTCondor ClassAds, where each 
item in the data list represents an HTCondor job.

Class Data has several methods to manipulate the data, 
but in all cases the output of the method is a new instance of one of the 
classes implemented: Data, _DictData, etc.
Methods never modify the current instance data.
This allows to perform different manipulations from the same source object.

There are two types of methods in class Data:

    - methods whose object output accepts further processing.
      Examples are methods indexby(), filter(), and map().

    - methods whose object output can not be processed anymore.
      An attempt to call any method on these instances
      will raise an Exception.
      Examples are methods reduce(), and process().

The method indexby() is somehow special. 
It is being used to split the stored data into a dictionary, 
according to whatever rule is provided. 
The values of this dictionary are themselves new Data instances. 
Therefore, the output of calling indexby() once is an _DictData object 
with data:
        
    self.data = {
                 key1: <Data>,
                 key2: <Data>,
                 ...
                 keyN: <Data>
                }

-------------------------------------------------------------------------------
                            Implementation
-------------------------------------------------------------------------------

The UML source for the classes is as follows:

        @startuml
        
        object <|-- _Base
        
        _Base <|-- _BaseDict 
        _Base <|-- Data 
        _Base <|-- _NonMutableData 

        _AnalysisInterface <|-- Data  
        _AnalysisInterface <|-- _DictData 

        _BaseDict <|-- _DictData 
        _BaseDict <|-- _NonMutableDictData 

        _GetRawBase <|-- Data 
        _GetRawBase <|-- _NonMutableData 
        
        @enduml


                                                            +--------+      
                                                            | object |
                                                            +--------+
                                                                ^
                                                                |
 +--------------------+                                     +-------+
 | _AnalysisInterface |    +------------------------------->| _Base |<-----------------+                  
 +--------------------+    |                                +-------+                  |
   ^                ^      |        +-------------+             ^               +-----------+              
   |                |      |        | _GetRawBase |             |               | _BaseDict |       
   |                |      |        +-------------+             |               +-----------+      
   |                |      |          ^        ^                |                 ^      ^     
   |                |      |          |        |                |                 |      |   
   |                |      |          |        |                |                 |      |   
   |                |      |          |        |                |                 |      |   
   |                |      |          |        |                |                 |      |   
   |            +==============+      |        |   +-----------------------+      |      |
   |            || Data       ||------+        +---| _NonMutableData       |      |      |
   |            +==============+                   +-----------------------+      |      |
   |                                    +-----------------+                       |  +---------------------------+
   +------------------------------------| _DictData       |-----------------------+  | _NonMutableDictData       |
                                        +-----------------+                          +---------------------------+


where Data is the only class truly part of the public API.


-------------------------------------------------------------------------------
                            Analyzers 
-------------------------------------------------------------------------------


The input to all methods is an object of type Analyzer. 
Analyzers are classes that implement the rules or policies to be used 
for each method call.  
For example: 
    - a call to method indexby() expects an object of type AnalyzerIndexBy
    - a call to method map() expects an object of type AnalyzerMap
    - a call to method reduce() expects an object of type AnalyzerReduce
    - etc.

Each Analyzer object must have implemented a method 
with the same name that the Data's method it is intended for. 
For exmple:

    - classes AnalyzerIndexBy must implement method indexby()
    - classes AnalyzerMap must implement method map()
    - classes AnalyzerReduce must implement method reduce()
    - ...


Passing an analyzer object that does not implement the right method will 
raise an IncorrectAnalyzer Exception.

Implementation of an indexby() method:
    - the input is an individual item from the list of data objects being analyzed
    - the output is the key under which this item will belong in the aggregated object

Implementation of a map() method:
    - the input is an individual item from the list of data objects being analyzed
    - the output is the modified item 

Implementation of a filter() method:
    - the input is an individual item from the list of data objects being analyzed
    - the output is a boolean indicating if the item should be kept or not

Implementation of a reduce() method:
    - the input is an individual item from the list of data objects being analyzed
    - the output is the aggregated result of analyzing the item and the previous value,
      which is being stored in a class attribute

Implementation of a transform() method:
    - the input is the entire list of data objects
    - the output is a new list of data object

Implementation of a process() method:
    - the input is the entire list of data objects
    - the output can be anything


    --------------------+----------------------------------------------------------------------------------------
    Container's method  | Analyzer Type       Analyzer's method   method's inputs    method's output
    --------------------+----------------------------------------------------------------------------------------
    indexby()           | AnalyzerIndexBy     indexby()           a data object      the key for the dictionary
    map()               | AnalyzerMap         map()               a data object      new data object
    filter()            | AnalyzerFilter      filter()            a data object      True/False
    reduce()            | AnalyzerReduce      reduce()            two data objects   new aggregated value
    transform()         | AnalyzerTransform   transform()         all data objects   new list of data object
    process()           | AnalyzerProcess     process()           all data objects   anything
    --------------------+----------------------------------------------------------------------------------------


A few basic pre-made Analyzers have been implemented, ready to use. 
"""

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

# =============================================================================
# ancillaries
# =============================================================================

def display(nested_dict, indent=0):
    """
    display the results of the processing
    """
    output = ""
    for key, value in nested_dict.items():
        output += " " * indent + str(key) + "\n"
        if isinstance(value, dict):
            output += display(value, indent + 4)
        elif isinstance(value, list):
            for item in value:
                output += " " * (indent + 4) + '%s' %str(item) + '\n'
        else:
            output += " " * (indent + 4) + str(value) + "\n"
    return output


# =============================================================================
#  Decorators 
#
#   Note:
#   the decorator must be implemented before the classes using it 
#   otherwise, they do not find it
# =============================================================================

def validate_call(method):
    """
    validates calls to the processing methods.
    Checks: 
        * if the Data object is mutable or not, 
        * if a method is being called with the right type of Analyzer
    Exceptions are raised with some criteria is not met.
    """
    def wrapper(self, analyzer, *k, **kw):
        method_name = method.__name__
        analyzertype = analyzer.analyzertype
        if not analyzertype == method_name:
            msg = 'Analyzer object {obj} is not type {name}. Raising exception.'
            msg = msg.format(obj = analyzer,
                             name = method_name)
            self.log.error(msg)
            raise IncorrectAnalyzer(analyzer, analyzertype, method_name)
        out = method(self, analyzer, *k, **kw)
        return out
    return wrapper


def catch_exception(method):
    """
    catches any exception during data processing
    and raises an AnalyzerFailure exception
    """
    def wrapper(self, analyzer):
        try:
            out = method(self, analyzer)
        except Exception as ex:
            msg = 'Exception of type "%s" ' %ex.__class__.__name__
            msg += 'with content "%s" ' %ex
            msg += 'while calling "%s" ' %method.__name__
            msg += 'with analyzer "%s"' %analyzer
            raise AnalyzerFailure(msg)
        else:
            return out
    return wrapper



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
        # 1
        tmp_new_data = {}
        for item in self.data:
            key_l = analyzer.indexby(item)
            if key_l is not None:
                if not isinstance(key_l, tuple):
                    # indexyby( ) may return a tuple or a single value
                    # in the second case, let's convert it into an interable
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
    def process(self, analyzer):
        new_data = {}
        for key, data in self.data.items(): 
            self.log.debug('calling process() for content in key %s'%key)
            new_data[key] = data.process(analyzer)
        new_info = _NonMutableDictData(new_data, timestamp=self.timestamp)
        return new_info


class _NonMutableData(_Base, _GetRawBase):
    pass

class _NonMutableDictData(_BaseDict):
    pass


# =============================================================================
# Analyzers
# =============================================================================

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


class AnalyzerProcess(Analyzer):
    analyzertype = "process"
    def process(self):
        """
        Implementation of a process() method:
            - the input is the entire list of data objects
            - the output can be anything
        """
        raise NotImplementedError


class Algorithm(object):
    """
    container for multiple Analyzer objects
    """
    def __init__(self):
        self.analyzer_l= []

    def add(self, analyzer):
        self.analyzer_l.append(analyzer)

    def analyze(self, input_data):
        tmp_out = input_data
        for analyzer in self.analyzer_l:
            tmp_out = tmp_out.analyze(analyzer)
        return tmp_out


# =============================================================================
# Exceptions
# =============================================================================

class IncorrectInputDataType(Exception):
    def __init__(self, type):
        self.value = 'Type of input data is not %s' %type
    def __str__(self):
        return repr(self.value)


class NotAnAnalyzer(Exception):
    def __init__(self):
        self.value = 'object does not have a valid analyzertype value'
    def __str__(self):
        return repr(self.value)


class IncorrectAnalyzer(Exception):
    def __init__(self, analyzer, analyzertype, methodname):
        value = "Analyzer object {ana} is of type '{atype}' but used for '{call}()'" 
        self.value = value.format(ana=analyzer, 
                                  atype=analyzertype, 
                                  call=methodname)
    def __str__(self):
        return repr(self.value)


class MissingKeyException(Exception):
    def __init__(self, key):
        self.value = "Key %s is not in the data dictionary" %key
    def __str__(self):
        return repr(self.value)


class AnalyzerFailure(Exception):
    """
    generic Exception for any unclassified failure
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


if __name__ == '__main__':
    # ============================================================================== 
    # fake example
    # ============================================================================== 
    
    class C(object):
        def __init__(self, name1, name2, value):
            self.name1 = name1
            self.name2 = name2
            self.value = value
    
    l = []
    l.append( C("foo", "test1", 4) ) 
    l.append( C("foo", "test2", 8) )
    l.append( C("bar", "test2", 8) )
    l.append( C("bar", "test2", 3) )
    l.append( C("bar", "test3", 1) )
    l.append( C("foo", "test3", 2) )
    l.append( C("foo", "test3", 2) )
    l.append( C("foo", "test1", 9) )
    l.append( C("bar", "test1", 9) )
    
    class TooLarge(AnalyzerFilter):
        def __init__(self, x):
            self.x = x
        def filter(self, c):
            return c.value <= self.x
    
    class ClassifyName1(AnalyzerIndexBy):
        def indexby(self, c):
            return c.name1
    
    class ClassifyName2(AnalyzerIndexBy):
        def indexby(self, c):
            if c.name2 == "test1":
                return "first"
            elif c.name2 == "test2":
                return "second"
            else:
                return "third"
    
    class Total(AnalyzerReduce):
        def reduce(self, v1, v2):
            if isinstance(v1, int):
                return v1 + v2.value
            else:
                return v1.value + v2.value
    
    data = Data(l)
    data = data.filter(TooLarge(5))
    data = data.indexby(ClassifyName1())
    data = data.indexby(ClassifyName2())
    data = data.reduce(Total(0))
    out = data.getraw()
    print(display(out))
    
    
    

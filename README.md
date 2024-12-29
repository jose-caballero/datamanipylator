Code to store and manipulate data.

# class Data

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

- methods whose object output accepts further processing. Examples are methods indexby(), filter(), and map().
- methods whose object output can not be processed anymore.  An attempt to call any method on these instances will raise an Exception. Examples are methods reduce(), and process().

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

# Implementation

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

This is the architecture:


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


## Analyzers 


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
- the output is the aggregated result of analyzing the item and the previous value, which is being stored in a class attribute

Implementation of a transform() method:
- the input is the entire list of data objects
- the output is a new list of data object

Implementation of a sort() method:
- the input are 2 Data objects
- the output is -1, 0 or 1, based on the implemented sorting rules

Implementation of a process() method:
- the input is the entire list of data objects
- the output can be anything



| **Container's method** | **Analyzer Type**     | **Analyzer's method** | **Method's inputs**  | **Method's output**           |
|------------------------|-----------------------|-----------------------|----------------------|-------------------------------|
| `indexby()`            | `AnalyzerIndexBy`     | `indexby()`           | a data object        | the key for the dictionary    |
| `map()`                | `AnalyzerMap`         | `map()`               | a data object        | new data object               |
| `filter()`             | `AnalyzerFilter`      | `filter()`            | a data object        | True/False                    |
| `reduce()`             | `AnalyzerReduce`      | `reduce()`            | two data objects     | new aggregated value          |
| `transform()`          | `AnalyzerTransform`   | `transform()`         | all data objects     | new list of data objects      |
| `sort()`               | `AnalyzerSort`        | `sort()`              | two data objects     | -1, 0, 1 (order)              |
| `process()`            | `AnalyzerProcess`     | `process()`           | all data objects     | anything                      |


A few basic pre-made Analyzers have been implemented, ready to use. 

# Other methods

| **Method's name** | **Analyzer Equivalent** | **Method's inputs** | **Method's output** |
|-------------------|-------------------------|---------------------|---------------------|
| `count()`         | Process                 | list                | len() of the list   |


# Fake example

Here is a fake example:

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

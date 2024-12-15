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
| `process()`            | `AnalyzerProcess`     | `process()`           | all data objects     | anything                      |


A few basic pre-made Analyzers have been implemented, ready to use. 

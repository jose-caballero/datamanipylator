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



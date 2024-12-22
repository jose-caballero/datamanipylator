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

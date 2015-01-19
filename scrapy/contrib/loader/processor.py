"""
This module provides some commonly used processors for Item Loaders.

See documentation in docs/topics/loaders.rst
"""

from scrapy.utils.misc import arg_to_iter
from scrapy.utils.datatypes import MergeDict
from .common import wrap_loader_context

class MapCompose(object):

    def __init__(self, *functions, **default_loader_context):
        self.functions = functions
        self.default_loader_context = default_loader_context

    def __call__(self, value, loader_context=None):
        values = arg_to_iter(value)
        if loader_context:
            context = MergeDict(loader_context, self.default_loader_context)
        else:
            context = self.default_loader_context
        wrapped_funcs = [wrap_loader_context(f, context) for f in self.functions]
        for func in wrapped_funcs:
            next_values = []
            for v in values:
                next_values += arg_to_iter(func(v))
            values = next_values
        return values


class Compose(object):

    def __init__(self, *functions, **default_loader_context):
        self.functions = functions
        self.stop_on_none = default_loader_context.get('stop_on_none', True)
        self.default_loader_context = default_loader_context

    def __call__(self, value, loader_context=None):
        if loader_context:
            context = MergeDict(loader_context, self.default_loader_context)
        else:
            context = self.default_loader_context
        wrapped_funcs = [wrap_loader_context(f, context) for f in self.functions]
        for func in wrapped_funcs:
            if value is None and self.stop_on_none:
                break
            value = func(value)
        return value


class TakeFirst(object):

    def __call__(self, values):
        for value in values:
            if value is not None and value != '':
                return value


class Identity(object):

    def __call__(self, values):
        return values


class JsonProcessor(object):
    """
        Given a jmespath, will search for it in the set of scraped values
        Requires : jmespath, ast modules
    """
    def __init__(self,jpath):
        self.json_path = jpath
        
    def __call__(self,values):
        jsonValues = []
        try:
            import jmespath,ast,json
        except:
            print "JsonProcessor module requires the jmespath library to function. No processing will happen in its abscence"
            return values
        for currentString in values:
            if currentString!="":#convert string to dict and then search
                jsonValue = jmespath.search(self.json_path,ast.literal_eval(currentString))
                if jsonValue:
                    if type(jsonValue) == dict:
                        jsonValues.append(json.dumps(jsonValue))
                    else:
                        jsonValues.append(jsonValue)                
        return jsonValues
        
class Join(object):

    def __init__(self, separator=u' '):
        self.separator = separator

    def __call__(self, values):
        return self.separator.join(values)

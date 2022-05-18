from .exceptions import SchemaError


class Compiler:
    def pre_compile(self, aql):
        components = aql.split()
        raise SchemaError()

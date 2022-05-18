from .exceptions import SchemaError, CommandNotFoundError
from .components import components

class Compiler:
    def pre_compile(self, aql):
        components = aql.split()
        print('=== resolving aql ===')
        i = 0
        while i<len(components):
            command = components[i].upper()
            if command in components:
                command = components[command]
                for component in command.pattern:
                    count, args = component.resolve(components[i:])
            else:
                raise CommandNotFoundError(command, components)
        print(components)
        # raise SchemaError()

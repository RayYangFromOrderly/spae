from .exceptions import SchemaError, CommandNotFoundError
from .components import commands

class Compiler:
    def pre_compile(self, aql):
        components = aql.split()
        print('=== resolving aql ===')
        i = 0
        while i<len(components):
            command = components[i].upper()
            command_args = []
            if command in commands:
                command = commands[command]
                i += 1
                for component in command.pattern:
                    i, args = component.resolve(components, i)
                    command_args += args
            else:
                raise CommandNotFoundError(command, list(commands.keys()))
        print(components)
        # raise SchemaError()

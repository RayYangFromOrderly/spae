from .exceptions import SchemaError, CommandNotFoundError
from .components import commands
from ..aggregation import Aggregation

class Compiler:
    def pre_compile(self, aql):
        aggregation = Aggregation()
        components = aql.split()
        print('=== resolving aql ===')
        i = 0
        while i<len(components):
            command = components[i].upper()
            if command in commands:
                command_args = []
                Command = commands[command]
                i += 1
                for component in Command.pattern:
                    i, args = component.resolve(components, i)
                    command_args += args

                command = Command(*command_args)
                command.simulate(aggregation)
            else:
                raise CommandNotFoundError(command, list(commands.keys()))
        print(components)
        # raise SchemaError()

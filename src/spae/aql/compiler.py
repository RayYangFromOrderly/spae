import re

from .exceptions import CommandNotFoundError
from .components import commands
from ..aggregation import Aggregation

class Compiler:
    def __init__(self, spae):
        self.spae = spae
        self.commands = []
        self.aql = None

    def pre_compile(self, aql):
        aggregation = Aggregation(self.spae)
        self.aql = aql
        self.commands = []
        components = [component for component in re.split('(\))|(\()|(,)| |(:)|\t|\n', aql) if component]
        print('=== resolving aql ===')
        i = 0
        while i<len(components):
            if components[i] == ';':
                continue

            command = components[i].upper()
            if command in commands:
                command_args = []
                Command = commands[command]
                i += 1
                for component in Command.pattern:
                    i, args = component.resolve(components, i)
                    command_args += args

                command = Command(*command_args)
                # command.simulate(aggregation)
                self.commands.append(command)
            else:
                raise CommandNotFoundError(command, list(commands.keys()))
        return self

    def run(self):
        aggregation = Aggregation(self.spae)
        for command in self.commands:
            command.run(aggregation)
        return aggregation.run()

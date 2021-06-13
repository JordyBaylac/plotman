import functools

import click
from pathlib import Path


class MadmaxCommands:
    def __init__(self):
        self.by_version = {}

    def register(self, version):
        if version in self.by_version:
            raise Exception(f'Version already registered: {version!r}')
        if not isinstance(version, tuple):
            raise Exception(f'Version must be a tuple: {version!r}')

        return functools.partial(self._decorator, version=version)

    def _decorator(self, command, *, version):
        self.by_version[version] = command
        # self.by_version = dict(sorted(self.by_version.items()))

    def __getitem__(self, item):
        return self.by_version[item]

    def latest_command(self):
        return max(self.by_version.items())[1]


commands = MadmaxCommands()

@commands.register(version=(0, 1))
@click.command()
@click.option("-n", "--count", help="Number of plots to create", type=int, default=1, show_default=True)
@click.option("-r", "--threads", help="Number of threads", type=int, default=4, show_default=True)
@click.option("-u", "--buckets", help="Number of buckets", type=int, default=256, show_default=True)
@click.option("-t", "--tmpdir", help="Temporary directory, needs ~220 GiB",
    type=click.Path(),
    default=Path("."),
    show_default=True,
)
@click.option("-2", "--tmpdir2", help="Temporary directory 2, needs ~110 GiB [RAM] (default = <tmpdir>)", type=click.Path(), default=Path("."))
@click.option("-d", "--finaldir",
    help="Final directory (default = <tmpdir>)",
    type=click.Path(),
    default=Path("."),
    show_default=True,
)
@click.option("-p", "--poolkey", help="Pool Public Key (48 bytes)", type=str, default=None)
@click.option("-f", "--farmerkey", help="Farmer Public Key (48 bytes)", type=str, default=None)
def _cli():
    pass
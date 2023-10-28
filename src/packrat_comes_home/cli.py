# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import logging
from argparse import ArgumentParser
from typing import Callable

from packrat_comes_home.populate import populate


def run():
    logging.basicConfig(level=logging.INFO)

    argument_parser = ArgumentParser()

    commands = argument_parser.add_subparsers(dest="command")

    populate_parser = commands.add_parser("populate")
    populate_parser.set_defaults(action=populate)

    arguments = argument_parser.parse_args()
    action = arguments.action

    import pdb

    pdb.set_trace()

    if not isinstance(action, Callable):
        raise ValueError(f"Unknown action {action}")
    action()

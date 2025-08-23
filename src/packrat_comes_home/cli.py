# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import logging
from argparse import ArgumentParser

from .download import download
from .log import logger, setup_logging
from .populate import populate


def run():
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        "--log-level", choices=logging.getLevelNamesMapping().keys(), default="INFO"
    )

    commands = argument_parser.add_subparsers(dest="command", required=True)

    populate_parser = commands.add_parser("populate")
    populate_parser.set_defaults(action=populate)

    download_parser = commands.add_parser("download")
    download_parser.set_defaults(action=download)

    arguments = argument_parser.parse_args()
    action = arguments.action

    setup_logging(level=arguments.log_level, log_path=f"{action.__name__}.log")

    try:
        action()
    except Exception as e:
        logger.exception("Exception: %s", e, exc_info=True)
        raise e

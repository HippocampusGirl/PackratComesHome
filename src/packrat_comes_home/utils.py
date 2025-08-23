# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from time import sleep

from dropbox.exceptions import InternalServerError
from requests import ConnectionError, ReadTimeout

from .log import logger


def robust_call(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except (ReadTimeout, ConnectionError, InternalServerError) as e:
            logger.error("Network error %s", exc_info=e)
            sleep(1e1)

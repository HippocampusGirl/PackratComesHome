#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from setuptools import find_packages, setup


if __name__ == "__main__":
    setup(
        name="packrat-comes-home",
        packages=find_packages(),
        entry_points=dict(
            console_scripts=[
                "packrat-comes-home = packrat_comes_home.cli:run",
            ],
        ),
        use_scm_version=dict(version_scheme="no-guess-dev"),
    )

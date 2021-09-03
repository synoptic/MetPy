# Copyright (c) 2015,2016,2017,2018 MetPy Developers.
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause
r"""Tools for requesting and ingesting data from remote sources"""

from .synoptic_data import *  # noqa: F403
from ..package_tools import set_module

__all__ = synoptic_data.__all__[:]  # pylint: disable=undefined-variable

set_module(globals())

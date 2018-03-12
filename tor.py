#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/9 18:31
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : tor.py
# @Software: PyCharm
# @Desc    :

from stem.control import Controller
from stem import Signal
def TorIDchange(port=9151):
    with Controller.from_port(port=port) as ctl:
        ctl.authenticate()
        ctl.signal(Signal.NEWNYM)

TorIDchange()
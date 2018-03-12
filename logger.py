#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : logger.py
# @Software: PyCharm
# @Desc    :
import os
import logging
import logging.handlers


logger = None

format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}

class Logger():
    __cur_logger = logging.getLogger()
    def __init__(self,loglevel):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), \
                                                                        'crawling.log'), maxBytes=500000000, backupCount=200)
        filehandler.setFormatter(formatter)
        new_logger.addHandler(filehandler)
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger

# logger = Logger(logging.DEBUG).getlogger()
logger = Logger(logging.INFO).getlogger()
# logger = Logger(logging.ERROR).getlogger()
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 05:38:21 2022

Run Data Query via LS P&L API (Whit)


@author: TQiu
"""


import pandas as pd
import datetime


def get_position_data(acids, asOfDate=None, outFPath=None, outFName=None):
    
    if asOfDate == None:
        today = datetime.date.today()
        if today.weekday() == 0: # Monday
            prevBD = today - datetime.timedelta(days = 3)
        elif today.weekday() == 6: # Sunday
            prevBD = today - datetime.timedelta(days = 2)
        else:
            prevBD = today - datetime.timedelta(days = 1)
        asOfDate = prevBD.strftime("%m/%d/%Y")
    date_str = datetime.datetime.strptime(asOfDate, "%m/%d/%Y").strftime("%Y%m%d")
    
    for acid in acids:
        df = pd.read_json('http://in2apps/riskperf/MTD/position.json?acid=' + str(acid) +'&asOfDate='+asOfDate)
        try:
            df1 = pd.DataFrame(df.transpose().data.values[0])
        except ValueError:
            df1 = pd.DataFrame(df.transpose().data)
        yield df, df1, acid, date_str


def get_detail_data(acids, asOfDate=None, outFPath=None, outFName=None):
    
    if asOfDate == None:
        today = datetime.date.today()
        if today.weekday() == 0: # Monday
            prevBD = today - datetime.timedelta(days = 3)
        elif today.weekday() == 6: # Sunday
            prevBD = today - datetime.timedelta(days = 2)
        else:
            prevBD = today - datetime.timedelta(days = 1)
        asOfDate = prevBD.strftime("%m/%d/%Y")
    date_str = datetime.datetime.strptime(asOfDate, "%m/%d/%Y").strftime("%Y%m%d")
    
    for acid in acids:
        df = pd.read_json('http://in2apps/riskperf/MTD/detail.json?acid=' + str(acid) +'&asOfDate='+asOfDate)
        try:
            df1 = pd.DataFrame(df.transpose().position.values[0])
        except ValueError:
            df1 = pd.DataFrame(df.transpose().position)
        yield df, df1, acid, date_str


__all__ = ["get_position_data","get_detail_data"]
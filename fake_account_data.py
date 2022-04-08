#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



import pymongo
from datetime import datetime, timedelta, date
import pandas as pd


import numpy as np
import pymongo
import cx_Oracle
import os
import time
import plotly.express as px
from datetime import datetime, timedelta, date

parent_path = '/home/server'


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None  # default='warn'


# %%





# %%





# %%


alfagift_otp_file = pd.read_csv(
    '/home/server/gli-data-science/akhiyar/alfagift_alert/otp_history/otp_hist.csv',dtype='object')


# %%


alfagift_otp_file.head()


# %%


import pymongo
myclient = pymongo.MongoClient(
    "mongodb://gli_user_view:h2y6ts7d@35.247.149.184:27017/?authSource=alfagift_message")
mydb = myclient["alfagift_message"]
mycol = mydb["alfagift_otp"]




alfagift_otp = pd.DataFrame(mycol.find({}))


# %%





# %%


wrap_col = ['_id', 'otp_code', 'valid_until', 'phone_num', 'action', 'deviceId', 'created_at', 'updated_at']
alfagift_otp_sel = alfagift_otp[wrap_col]

now_str=str(datetime.now().strftime('%d%b%y_%H%M'))

alfagift_otp_save = pd.concat([alfagift_otp_file,alfagift_otp_sel])
alfagift_otp_save = alfagift_otp_save.drop_duplicates()
alfagift_otp_save.to_csv(
    '/home/server/gli-data-science/akhiyar/alfagift_alert/otp_history/otp_hist.csv', index=False)


# %%


# alfagift_otp_save['valid_until'].max()


# %%


# alfagift_otp_save['valid_until'].min()


# %%





# %%


print(now_str)


# %%





# %%





# %%





# %%





#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import pandas as pd
pd.options.mode.chained_assignment = None
import psycopg2

from datetime import datetime, timedelta

import os
import sys
import shutil
from os.path import basename
from pathlib import Path



import lib_3d
import telegram
import pandas as pd
from telegram import ParseMode

curdir = os.getcwd()
otd = os.path.join(curdir)

path_hist = '/home/server/gli-data-science/akhiyar/alfagift_alert/hist_voucher.csv'

# try:
#     cpo = psycopg2.connect(host="35.240.137.10",
#                             database="prd_order",
#                             user="akhiyar_waladi",
#                             password="nd4n6fk9")
# except Exception as error:
#     print("Error while connecting: ", error)
#     sys.exit()


import warnings
import sys
import os
sys.path.append('/home/server/gli-data-science/')
if not sys.warnoptions:
    warnings.simplefilter("ignore")
    os.environ["PYTHONWARNINGS"] = "ignore" # Also affect subprocesses

import ds_db

cpo, cursor = ds_db.connect_prd_order_4()


# %%


# define now date
t1 = datetime.now()
t1_now = datetime.strftime(t1, '%Y-%m-%d %H:%M')
# define lower bound of window time that we want to check
t2 = (t1 - timedelta(minutes=240))
t2_window = datetime.strftime(t2,'%Y-%m-%d %H:%M')
# define 7 week before lower bound of window that we suspect
t2_frame = datetime.strftime((t2.date() - timedelta(days=30)),'%Y-%m-%d %H:%M')

print("check time {} {}".format(t2_window, t1_now))
print("past time {} {}".format(t2_frame, t2_window))


# getting all order with specific of time bound
main = """
    select 
        tbto_voucher_code, tbto_ponta_id, tbto_no, tbto_voucher_usage, tbto_create_date
    from 
        tb_transaction_order tto 
    where 
        tbto_create_date between %s and %s
        and tbto_status not in ('18','10','11')
        and tbto_voucher_code not in ('')
"""

df_window = pd.read_sql_query(main,cpo,params=[t2_window,t1_now])
df_frame = pd.read_sql_query(main,cpo,params=[t2_frame,t2_window])


# %%


df_window_g = df_window.groupby('tbto_voucher_code').agg({'tbto_no':'count'}).reset_index()
same_voucher_window = list(df_window_g[df_window_g['tbto_no'] > 1]['tbto_voucher_code'])
same_voucher_frame = list(set(df_window['tbto_voucher_code']) & set(df_frame['tbto_voucher_code']))

same_voucher = same_voucher_window + same_voucher_frame
df = pd.concat([df_window, df_frame])

# groupby voucher code and count order number that used, if same voucher empty this would not affect
dfa = df[df['tbto_voucher_code'].isin(same_voucher)].groupby('tbto_voucher_code').agg({'tbto_no':'count'})
dfa = dfa.rename(columns = {'tbto_no':'voucher_usage'}).reset_index()
dfa = dfa[dfa['voucher_usage']>=2]


# %%





# %%


dfa_hist = pd.read_csv(path_hist)
dfa_hist['date'] = pd.to_datetime(dfa_hist['date'])


# %%


# voucher code that sent to alert must not same as 
dfa = dfa[~dfa['tbto_voucher_code'].isin(dfa_hist['tbto_voucher_code'])]
dfa['date'] = t1


# %%





# %%


dfa_hist = pd.concat([dfa_hist, dfa])
dfa_hist = dfa_hist[dfa_hist['date'] >= t1 - timedelta(minutes=240)]
dfa_hist.to_csv(path_hist, index=False)
dfa = dfa.drop('date', axis=1)


# %%





# %%





# %%





# %%


if len(dfa) > 0:
    
    # mechanism to send email
#     lib = lib_3d.desan()
#     preceiver = "product.operation@gli.id, william.d.sinolungan@gli.id,                 akhiyar.waladi@gli.id"

#     #preceiver = "akhiyar.waladi@gli.id"
#     print(preceiver)

#     email_date = t1.strftime('%d%b%y %H:%M')
#     psubject = 'Alfagift Alert [{}]'.format(email_date)

#     x=dfa.to_html()
#     pbody = """Time between {} and {} there is an abnormal transaction, please check below <br><hr><br> Using vouchers that have been used before {}""".format(t2_window,t1_now,x)
#     lib.kirim_email_noreply(preceiver, psubject, pbody, "")


    
    # telegram send message
    bot = telegram.Bot(token='1539145464:AAEZKQzDhEwir3x5PDLzYKHxLC-2Igc7Gyo')

    x_m = dfa.rename(columns={'voucher_usage':'usage'}).to_markdown(index=False, tablefmt="grid")
    head_chat = 'Using vouchers that have been used before\n{} --> {}'.format(t2_window,t1_now)

    bot.send_message(chat_id='-1001309547292', text="{}\n\n<pre>{}</pre>".format(head_chat, x_m),                     parse_mode=ParseMode.HTML)


# %%


cpo.close()


# %%





# %%





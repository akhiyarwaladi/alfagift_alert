#!/usr/bin/env python
# coding: utf-8
# %%

# %%
import psycopg2
import pymongo
import pandas as pd
pd.options.mode.chained_assignment = None 

import lib_3d
import sys
from datetime import datetime, timedelta, date

import telegram
from telegram import ParseMode

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

import warnings
import sys
import os
sys.path.append('/home/server/gli-data-science/')
if not sys.warnoptions:
    warnings.simplefilter("ignore")
    os.environ["PYTHONWARNINGS"] = "ignore" # Also affect subprocesses

import ds_db

# %%
dr_order = (datetime.now()).replace(second=0, microsecond=0)
message = []

# %%



b1, b2 = False, False
m1, m2 = False, False

if ((dr_order.hour) in (list(range(0,24)))) and ((dr_order.minute) == 0):
    b1 = True
if ((dr_order.hour) in (list(range(6,24)))) and ((dr_order.minute) % 15 == 0):
    b2 = True

## for testing purpose uncomment
# b1, b2 = False, True


# %%
print(dr_order, b1, b2)


# %%

# %%



if b1:
    
    try:
        q_1 = '''
            select tto.tbto_id, tto.tbto_application_id 
            from tb_transaction_order tto
            where tto.tbto_create_date between '{shift_str}' and '{now_str}'
            and tto.tbto_status = 12

        '''.format(shift_str = (dr_order-timedelta(hours=1)), now_str = dr_order)

        connection, cursor = ds_db.connect_prd_order_4()
        res_order = pd.read_sql(q_1, connection)
        res_order = res_order.dropna().astype(int)
        connection.close()

        ## testing purpose
        #res_order = res_order[res_order['tbto_application_id'].isin([402])]
        if len(res_order) == 0:

            if ((dr_order.hour) in (list(range(1,5)))):
                if len(res_app) == 0:
                    message.append('{} -- {}'.format(str(dr_order-timedelta(hours=1)), str(dr_order)))
                    message.append('There is no transaction [status=12] on app order [app_id=904,905]')
                    m1 = True
            if ((dr_order.hour) in (list(range(6,24)))):
                if len(res_web) == 0:
                    message.append('{} -- {}'.format(str(dr_order-timedelta(hours=1)), str(dr_order)))
                    message.append('There is no transaction [status=12] on web order [app_id=402]')
                    m1 = True

        else:
            res_app = res_order[res_order['tbto_application_id'].isin([904,905])]
            res_web = res_order[res_order['tbto_application_id'].isin([402])]

            print(res_app.drop_duplicates(subset=['tbto_application_id']))
            print(res_web.drop_duplicates(subset=['tbto_application_id']))

            if ((dr_order.hour) in (list(range(1,5)))):
                if len(res_app) == 0:
                    message.append('{} -- {}'.format(str(dr_order-timedelta(hours=1)), str(dr_order)))
                    message.append('There is no transaction [status=12] on app order [app_id=904,905]')
                    m1 = True
            if ((dr_order.hour) in (list(range(6,24)))):
                if len(res_web) == 0:
                    message.append('{} -- {}'.format(str(dr_order-timedelta(hours=1)), str(dr_order)))
                    message.append('There is no transaction [status=12] on web order [app_id=402]')
                    m1 = True

    
    except Exception as e:
        print(e)



# %%



if b2:
    
    try:
        q_1 = '''
            select tto.tbto_id, tto.tbto_application_id 
            from tb_transaction_order tto
            where tto.tbto_create_date between '{shift_str}' and '{now_str}'
            and tto.tbto_status = 12

        '''.format(shift_str = (dr_order-timedelta(minutes=15)), now_str = dr_order)

        connection, cursor = ds_db.connect_prd_order_4()
        res_order = pd.read_sql(q_1, connection)
        res_order = res_order.dropna().astype(int)
        connection.close()

        ## testing purpose
        #res_order = res_order[res_order['tbto_application_id'].isin([402])]
        if len(res_order) == 0:
            message.append('{} -- {}'.format(str(dr_order-timedelta(minutes=15)), str(dr_order)))
            message.append('There is no transaction [status=12] on app order [app_id=904,905]')

            m2 = True

        else:
            res_app = res_order[res_order['tbto_application_id'].isin([904,905])]
            res_web = res_order[res_order['tbto_application_id'].isin([402])]

            print(res_app.drop_duplicates(subset=['tbto_application_id']))
            print(res_web.drop_duplicates(subset=['tbto_application_id']))
            if len(res_app) == 0:
                message.append('{} -- {}'.format(str(dr_order-timedelta(minutes=15)), str(dr_order)))
                message.append('There is no transaction [status=12] on app order [app_id=904,905]')
                m2 = True

    except Exception as e:
        print(e)
        sys.exit(e)

    



# %%


print("m1:{} m2:{}".format(m1,m2))


# %%
if m1 or m2:
    
    out_format = ''
    for idx, out_message in enumerate(message):
        out_format += '{}<br>'.format(out_message)
        if idx == 0:
            out_format += '<br>'
            
            
    # mechanism to send email
    email_date = dr_order.strftime('%d%b%y %H:%M')
    lib = lib_3d.desan()
    preceiver = "product.operation@gli.id, william.d.sinolungan@gli.id, akhiyar.waladi@gli.id"

    #preceiver = "akhiyarwaladi@gmail.com"
    print(preceiver)


    psubject = 'Alfagift Alert [{}]'.format(email_date)
    pbody = """{}""".format(out_format)

    lib.kirim_email_noreply(preceiver, psubject, pbody, "")
    
    
    
    # telegram send message
    bot = telegram.Bot(token='1539145464:AAEZKQzDhEwir3x5PDLzYKHxLC-2Igc7Gyo')

    out_format = ''
    for idx, out_message in enumerate(message):
        out_format += '{} \n'.format(out_message)
        if idx == 0:
            out_format += '\n'

        


    bot.send_message(chat_id='-1001309547292', text="{}".format(out_format), parse_mode=ParseMode.HTML)


# %%



connection.close()

# %%





# %%





# %%

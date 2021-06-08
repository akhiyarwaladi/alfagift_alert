#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd
pd.options.mode.chained_assignment = None 
import lib_3d
from datetime import datetime, timedelta

import telegram
from telegram import ParseMode

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

try:
    connection = psycopg2.connect(
        host="35.187.250.81",
        database="prd_order",
        user="akhiyar_waladi",
        password="nd4n6fk9")

except (Exception, psycopg2.Error) as error :
    print("Error while connecting to PostgreSQL", error)


# In[ ]:





# In[2]:


print(datetime.now())


# In[3]:


const_out_order = 600
const_out_voucher_amount = 2000000
const_out_voucher_count = 20
const_out_point_issue = 100000
const_out_point_redeem = 100000


b1, b2, b3, b4 = False, False, False, False
m1, m2, m3, m4 = False, False, False, False

if (datetime.now().hour) in [1, 2, 3, 4, 6]:
    b1 = True
if (datetime.now().minute) % 30 == 0:
    b3, b4 = True, True
if (datetime.now().hour) == 13:
    b2 = True

# b1, b2, b3, b4 = True, True, True, True

# const_out_order = 500
# const_out_voucher_amount = 300000
# const_out_voucher_count = 10
# const_out_point_issue = 2000
# const_out_point_redeem = 20000


# In[4]:


print(b1, b2, b3, b4)


# In[5]:


dr_order = (datetime.now()).replace(second=0)
dr_voucher = datetime.now().date()
dr_redeem_issue = (datetime.now()).replace(second=0)


# In[ ]:





# In[6]:


out_count_order = ''
if b1:
    q_1 = '''
        select count(tbto_no)
        from tb_transaction_order tto 
        where tto.tbto_create_date between '{shift_str}' and '{now_str}'
        and tbto_status is not null
    '''.format(shift_str = (dr_order-timedelta(hours=1)), now_str = dr_order)

    count_order = pd.read_sql(q_1, connection)
    
    out_count_order = count_order[count_order['count'] > const_out_order]
    
    if len(out_count_order) > 0:
        out_count_order['count'] = out_count_order['count']                    .astype(int).apply(lambda x : "{:,}".format(x))
        m1 = True


# In[ ]:





# In[ ]:





# In[7]:


out_voucher_check = ''
out_voucher_check_2 = ''
if b2:
    q_2 = '''
    -- check voucher amount usage and voucher code count in one day interval
    select 
        tbto_ponta_id, sum(tbto_voucher_usage) as sum_voucher_usage
    from 
        tb_transaction_order tto 
    where 
        tto.tbto_create_date between '{shift_str}' and '{now_str}' 
        and tbto_ponta_id is not null
        and tbto_voucher_usage is not null
        and tto.tbto_status not in ('18','10','11')
        and tbto_voucher_code not in ('')
        and tbto_status is not null
    group by tbto_ponta_id
    order by sum_voucher_usage desc

    '''.format(shift_str=str(dr_voucher-timedelta(days=1)), now_str=str(dr_voucher))
    
    voucher_check = pd.read_sql(q_2, connection)
    
    
    q_5 = '''
    -- check voucher amount usage and voucher code count in one day interval
    select 
        tbto_ponta_id, count(distinct(tbto_voucher_code)) as count_unique_voucher
    from 
        tb_transaction_order tto 
    where 
        tto.tbto_create_date between '{shift_str}' and '{now_str}' 
        and tbto_ponta_id is not null
        and tbto_voucher_usage is not null
        and tto.tbto_status not in ('18','10','11')
        and tbto_voucher_code not in ('')
        and tbto_status is not null
    group by tbto_ponta_id
    order by count_unique_voucher desc

    '''.format(shift_str=str(dr_voucher-timedelta(days=1)), now_str=str(dr_voucher))
    
    voucher_check_2 = pd.read_sql(q_5, connection)
    
    out_voucher_check = voucher_check[voucher_check['sum_voucher_usage'] >= const_out_voucher_amount]
    out_voucher_check_2 = voucher_check_2[voucher_check_2['count_unique_voucher'] >= const_out_voucher_count]
    
    
    if (len(out_voucher_check) > 0) or (len(out_voucher_check_2) > 0):
        out_voucher_check['sum_voucher_usage'] = out_voucher_check['sum_voucher_usage']                            .astype(int).apply(lambda x : "{:,d}".format(x))
        out_voucher_check_2['count_unique_voucher'] = out_voucher_check_2['count_unique_voucher'].astype('int')
        m2 = True


# In[ ]:





# In[8]:


out_point_redeem = ''
if b3:
    q_3 = '''
    -- check trx with ponta redeemed and issued
    select 
        tto.tbto_ponta_id, sum(ttp.tbtp_ponta_amount) as sum_ponta_redeem, count(tto.tbto_id) as count_order_id
    from 
        tb_transaction_order tto 
        left join tb_transaction_payment ttp on tto.tbto_id = ttp.tbto_id
    where 
        tto.tbto_create_date between '{shift_str}' and '{now_str}'
        and ttp.tbmp_id = 23
        and tto.tbto_status not in ('18','10','11')
        and tbto_status is not null
    group by tto.tbto_ponta_id 
    order by sum_ponta_redeem desc
    '''.format(shift_str=str(dr_redeem_issue-timedelta(minutes=30)), now_str=str(dr_redeem_issue))
    point_redeem = pd.read_sql(q_3, connection)
    
    out_point_redeem = point_redeem[point_redeem['sum_ponta_redeem'] > const_out_point_redeem]
    if len(out_point_redeem) > 0:
        out_point_redeem['sum_ponta_redeem'] = out_point_redeem['sum_ponta_redeem']                    .astype(int).apply(lambda x : "{:,}".format(x))
        m3 = True


# In[ ]:





# In[9]:


out_point_issue = ''
if b4:
    q_4 = '''
    -- check trx with ponta redeem and issued
    select 
        tto.tbto_ponta_id, sum(tto.tbto_ponta_point) as sum_ponta_issued, count(tto.tbto_id) as count_order_id
    from 
        tb_transaction_order tto 
    where 
        tto.tbto_create_date between '{shift_str}' and '{now_str}'
        and tbto_ponta_id is not null
        and tto.tbto_status not in ('18','10','11')
        and tbto_status is not null
    group by tto.tbto_ponta_id 
    order by sum_ponta_issued desc
    '''.format(shift_str=str(dr_redeem_issue-timedelta(minutes=30)), now_str=str(dr_redeem_issue))
    point_issue = pd.read_sql(q_4, connection)
    
    out_point_issue = point_issue[point_issue['sum_ponta_issued'] > const_out_point_issue]
    
    if len(out_point_issue) > 0:
        out_point_issue['sum_ponta_issued'] = out_point_issue['sum_ponta_issued']                    .astype(int).apply(lambda x : "{:,d}".format(x))
        m4 = True


# In[ ]:





# In[10]:


print(m1,m2,m3,m4)


# In[ ]:





# In[11]:


if m1 or m2 or m3 or m4:
    
    outdf_format = ''
    body_format = [
        'Number of order in last 1 hour',
        'Voucher usage amount sum in last 1 day',
        'Voucher usage count sum in last 1 day',
        'Point redeem sum (using point) in last 30 minutes',
        'Point issue sum (get point) in last 30 minutes',
    ]
    for idx, outdf in enumerate([out_count_order, out_voucher_check, out_voucher_check_2, out_point_redeem, out_point_issue]):
        if len(outdf) > 0:
            outdf_format += '{} <br> {} <br><hr><br>'.format(body_format[idx], outdf.to_html())
            
            
    # mechanism to send email
    email_date = dr_order.strftime('%d%b%y %H:%M')
    lib = lib_3d.desan()
    preceiver = "product.operation@gli.id, william.d.sinolungan@gli.id,                 akhiyar.waladi@gli.id"

#     preceiver = "akhiyarwaladi@gmail.com"
    print(preceiver)


    psubject = 'Alfagift Alert [{}]'.format(email_date)
    pbody = """Time {} there is an abnormal transaction, please check below <br><hr><br> {}""".format(email_date, outdf_format)

    lib.kirim_email_noreply(preceiver, psubject, pbody, "")
    
    
    
    # telegram send message
    bot = telegram.Bot(token='1539145464:AAGEJ4OjCNTGhAOYi2bRsqkiSVI2Ntt4ndo')

    outdf_format = ''
    body_format = [
        'Number of order in last 1 hour',
        'Voucher usage amount sum in last 1 day',
        'Voucher usage count sum in last 1 day',
        'Point redeem sum (using point) in last 30 minutes',
        'Point issue sum (get point) in last 30 minutes',
    ]
    if len(out_voucher_check) > 0:
        out_voucher_check = out_voucher_check.rename(columns={'sum_voucher_usage':'sum'})
    if len(out_voucher_check_2) > 0:
        out_voucher_check_2 = out_voucher_check_2.rename(columns={'count_unique_voucher':'count'})
    if len(out_point_redeem) > 0:
        out_point_redeem = out_point_redeem.rename(columns={'sum_ponta_redeem':'sum','count_order_id':'c'}).drop('c',1)
    if len(out_point_issue) > 0:
        out_point_issue = out_point_issue.rename(columns={'sum_ponta_issued':'sum','count_order_id':'c'}).drop('c',1)
    for idx, outdf in enumerate([out_count_order, out_voucher_check, out_voucher_check_2, out_point_redeem, out_point_issue]):
        if len(outdf) > 0:
            outdf_format += '{} {} \n\n<pre>{}</pre>\n\n------------------------------------------------------------------\n\n'            .format(email_date, body_format[idx], outdf.to_markdown(index=False, tablefmt="grid"))



    bot.send_message(chat_id='@alfagift_alert', text="{}".format(outdf_format),                     parse_mode=ParseMode.HTML)

#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd
import lib_3d
from datetime import datetime, timedelta

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


const_out_order = 1000
const_out_voucher_amount = 1000000
const_out_voucher_count = 10
const_out_point_issue = 50000
const_out_point_redeem = 50000


b1, b2, b3, b4 = False, False, False, False
m1, m2, m3, m4 = False, False, False, False

if (datetime.now().hour) in [2, 3, 4, 5, 6]:
    b1 = True
if (datetime.now().minute) % 30 == 0:
    b3, b4 = True, True
if (datetime.now().hour) == 7:
    b2 = True

# b1, b2, b3, b4 = True, True, True, True


# In[ ]:





# In[4]:


dr_order = (datetime.now()).replace(minute=0, second=0)
dr_voucher = datetime.now().date()
dr_redeem_issue = (datetime.now()).replace(second=0)


# In[ ]:





# In[5]:


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
        m1 = True


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[6]:


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
        and tbto_status is not null
    group by tbto_ponta_id
    order by count_unique_voucher desc

    '''.format(shift_str=str(dr_voucher-timedelta(days=1)), now_str=str(dr_voucher))
    
    voucher_check_2 = pd.read_sql(q_5, connection)
    
    out_voucher_check = voucher_check[voucher_check['sum_voucher_usage'] >= const_out_voucher_amount]
    out_voucher_check_2 = voucher_check_2[voucher_check_2['count_unique_voucher'] >= const_out_voucher_count]
    
    if (len(out_voucher_check) > 0) or (len(out_voucher_check_2) > 0):
        m2 = True


# In[ ]:





# In[ ]:





# In[7]:


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
        m3 = True


# In[ ]:





# In[8]:


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
        m4 = True


# In[ ]:





# In[9]:


print(m1,m2,m3,m4)


# In[ ]:





# In[10]:


if m1 or m2 or m3 or m4:
    
    outdf_format = ''
    body_format = [
        'Num of order in last 1 hour',
        'Voucher usage amount sum in last 1 day',
        'Voucher usage count sum in last 1 day',
        'Point redeem sum in last 30 minutes',
        'Point issue sum in last 30 minutes',
    ]
    for idx, outdf in enumerate([out_count_order, out_voucher_check, out_voucher_check_2, out_point_redeem, out_point_issue]):
        if len(outdf) > 0:
            outdf_format += '{} <br> {} <br><hr><br>'.format(body_format[idx], outdf.to_html())
    # mechanism to send email


    email_date = dr_order.strftime('%d%b%y %H:%M')
    lib = lib_3d.desan()
    # preceiver = "benny.chandra@gli.id, erick.alviyendra@gli.id, reinaldo@gli.id, \
    #             prasistyo.utomo@gli.id, kevin.runtupalit@gli.id, \
    #             dita.rahmawati@gli.id, william.d.sinolungan@gli.id, \
    #             akhiyar.waladi@gli.id"

    preceiver = "akhiyar.waladi@gli.id, william.d.sinolungan@gli.id"
    print(preceiver)


    psubject = 'Alfagift Alert [{}]'.format(email_date)
    pbody = """Time {} there is an abnormal transaction, please check below <br> <br> {}""".format(email_date, outdf_format)

    lib.kirim_email_noreply(preceiver, psubject, pbody, "")


# In[ ]:





# In[ ]:





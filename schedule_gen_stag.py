#!/usr/bin/env python
# coding: utf-8
# %%

# %%
import psycopg2
import pymongo
import pandas as pd
pd.options.mode.chained_assignment = None 
import lib_3d
from datetime import datetime, timedelta, date

import telegram

from telegram import ParseMode

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# try:
#     connection = psycopg2.connect(
#         host="35.187.250.81",
#         database="prd_order",
#         user="akhiyar_waladi",
#         password="nd4n6fk9")

# except (Exception, psycopg2.Error) as error :
#     print("Error while connecting to PostgreSQL", error)


import warnings
import sys
import os
sys.path.append('/home/server/gli-data-science/')
if not sys.warnoptions:
    warnings.simplefilter("ignore")
    os.environ["PYTHONWARNINGS"] = "ignore" # Also affect subprocesses

import ds_db


from sqlalchemy import event,create_engine,types
driver = 'cx_oracle'
server = '10.234.152.61' 
database = 'alfabi' 
username = 'report' 
password = 'justd0it'
engine_stmt = "oracle://%s:%s@%s/%s" % (username, password, server, database )


import cx_Oracle    
# def connect_alfabi():
#     try:
#         conn_str = u'report/justd0it@10.234.152.61:1521/alfabi'
#         connection_alfabi = cx_Oracle.connect(conn_str)
#     except Exception as e:
#         print(e)
        
#     return connection_alfabi


# %%





# %%
const_out_order = 600
const_out_voucher_amount = 2000000
const_out_voucher_count = 20
const_out_point_issue = 100000
const_out_point_redeem = 1000000


b1, b2, b3, b4, b5 = False, False, False, False, False
m1, m2, m3, m4 = False, False, False, False

if (datetime.now().hour) in [1, 2, 3, 4]:
    b1 = True
if (datetime.now().minute) % 30 == 0:
    b3, b4 = True, True
# if ((datetime.now().hour) == 16) and ((datetime.now().minute)) == 30:
#     b2 = True
if ((datetime.now().hour) == 9):
    b2 = True
if ((datetime.now().hour) == 13) and ((datetime.now().minute)) == 0 and ((datetime.now().day)) == 1:
    b5 = True

# b1, b2, b3, b4 = False, True, True, True

# const_out_order = 500
# const_out_voucher_amount = 300000
# const_out_voucher_count = 10
# const_out_point_issue = 2000
# const_out_point_redeem = 20000


# %%


print(datetime.now(), b1, b2, b3, b4, b5)


# %%


dr_order = (datetime.now()).replace(second=0)
dr_voucher = datetime.now().date()
dr_redeem_issue = (datetime.now()).replace(second=0)


# %%

# %%
dr_order = (datetime.now()).replace(second=0)
start_look = dr_order-timedelta(minutes=15)
end_look = dr_order

# %%
start_look

# %%
end_look

# %%


out_count_order = ''
if b1:
    q_1 = '''
        select count(tbto_no)
        from tb_transaction_order tto 
        where tto.tbto_create_date between '{shift_str}' and '{now_str}'
        and tbto_status is not null
        and tbto_status in ('12','14','15')
    '''.format(shift_str = (dr_order-timedelta(hours=1)), now_str = dr_order)

    count_order = pd.read_sql(q_1, connection)
    
    out_count_order = count_order[count_order['count'] > const_out_order]
    
    if len(out_count_order) > 0:
        out_count_order['count'] = out_count_order['count']                    .astype(int).apply(lambda x : "{:,}".format(x))
        m1 = True


# %%





# %%
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
    
    connection, cursor = ds_db.connect_prd_order_4()
    voucher_check = pd.read_sql(q_2, connection)
    connection.close()
    
    
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
    
    connection, cursor = ds_db.connect_prd_order_4()
    voucher_check_2 = pd.read_sql(q_5, connection)
    connection.close()
    
    out_voucher_check = voucher_check[voucher_check['sum_voucher_usage'] >= const_out_voucher_amount]
    out_voucher_check_2 = voucher_check_2[voucher_check_2['count_unique_voucher'] >= const_out_voucher_count]

    
    if len(out_voucher_check_2) > 0:
        if len(out_voucher_check_2) == 1:
            tup_mem = tuple(list(out_voucher_check_2['tbto_ponta_id']) + ['1'])
        else:
            tup_mem = tuple(out_voucher_check_2['tbto_ponta_id'])
            
        q='''
        SELECT 
            NO_KARTU, 
            NAMA_LENGKAP, 
            NIK_KARYAWAN
        FROM 
            MASTER_CUST mc 
        WHERE 
            NO_KARTU in {}

        '''.format(tup_mem)
        con = ds_db.connect_alfabi()
        mc = pd.read_sql(q, con)
        con.close()

        out_voucher_check_2_save = pd.merge(out_voucher_check_2, mc, left_on='tbto_ponta_id', 
                                       right_on='NO_KARTU', how='left').fillna('-').drop('NO_KARTU',1)  
        
        out_voucher_check_2_save = out_voucher_check_2_save.rename(columns={'tbto_ponta_id':'ponta_id', 
                    'count_unique_voucher':'count', 'NAMA_LENGKAP':'nama', 'NIK_KARYAWAN':'NIK'})



        ponta_id = tuple(map(str,list(out_voucher_check_2['tbto_ponta_id'])+['1']))


        
        query = '''
        select 
            tto.tbto_no, tto.tbto_create_date, tto.tbto_ponta_id, tto.tbto_voucher_code, tto.tbto_voucher_usage
        from 
            tb_transaction_order tto 
        where 
            tto.tbto_create_date between '{shift_str}' and '{now_str}' 
            and tbto_ponta_id is not null
            and tbto_voucher_usage is not null
            and tto.tbto_status not in ('18','10','11')
            and tto.tbto_voucher_code not in ('')
            and tto.tbto_status is not null
            and tto.tbto_ponta_id in {ponta_id}
        '''.format(shift_str=str(dr_voucher-timedelta(hours=30)), now_str=str(dr_voucher), ponta_id=ponta_id)
        
        connection, cursor = ds_db.connect_prd_order_4()
        results = pd.read_sql_query(query, connection)
        connection.close()
      
        
        df = results[['tbto_voucher_code']]
        li_vcr = list(results['tbto_voucher_code'])

        engine = create_engine(engine_stmt)


        dtyp = {c:types.VARCHAR(df[c].str.len().max()) for c in df.columns[df.dtypes == 'object'].tolist()}
        df.to_sql('temp_voucher', engine, index=False, 
                                                  if_exists="replace", dtype=dtyp)


        engine.dispose()



        try:
            ## find voucher in master voucher using voucher code to get reward id
            koneksi4 = pymongo.MongoClient\
            ("mongodb://user_read:read12345678@35.240.152.164:27017/alfagift_loyalty_promotion")
            mydb = koneksi4["alfagift_loyalty_promotion"]
            mycol = mydb["alfagift_master_voucher"]


            rew_vcr = pd.DataFrame(mycol.find({'reg_code':{"$in":li_vcr}}))
            rew_vcr = rew_vcr[['reward_id','reg_code','voucher_used_by','ponta_id']]
            from bson.objectid import ObjectId
            li_rew = [ObjectId(rew) for rew in list(rew_vcr['reward_id'])]
            ##

            ## find rewad in master reward to get detail campaign
            koneksi4 = pymongo.MongoClient\
                    ("mongodb://user_mkt:us3r_mkt@35.213.177.53:27017/alfagift_cms")
            mydb = koneksi4["alfagift_cms"]
            mycol = mydb["alfagift_master_reward"]


            rew_det = pd.DataFrame(mycol.find({'_id':{"$in":li_rew}}))
            rew_det = rew_det[['_id','reward_name','reward_start_date','reward_end_date','voucher_type'
                               ,'created_at','created_by','created_by_mail']]
            rew_det['_id'] = rew_det['_id'].astype(str)
            ##

            ## merging master voucher and campaign detail
            rew_merge = pd.merge(rew_vcr,rew_det, left_on='reward_id', right_on='_id', how='left').drop('_id',1)


        except Exception as e:
            print(e)
            rew_merge=pd.DataFrame()
            pass
        try:
            if len(li_vcr) == 1:
                tup_vcr = tuple(li_vcr+['1'])
            else:
                tup_vcr = tuple(li_vcr)
            query = '''
                SELECT 
                    est.ELS_RECEIPT, 
                    est.ELS_SMS_REGISTRATION_CODE, 
                    esmp.MSO_DESCP, 
                    esmp.MSO_KET, 
                    esmp.MSO_CREATE_USER 
                FROM 
                    TEMP_VOUCHER tv 
                    LEFT JOIN ELS_SMS_TRANS est ON tv.TBTO_VOUCHER_CODE = est.ELS_SMS_REGISTRATION_CODE 
                    LEFT JOIN ELS_SMS_MS_PROMO esmp ON est.ELS_MSO_ID  = esmp.MSO_ID 


            '''.format(tup_vcr)
            con_alfabi = ds_db.connect_alfabi()
            results_alfabi = pd.read_sql_query(query, con_alfabi)


            con_alfabi.close()

            create_user_map={'1012115509':'SUGIYONO',
            '1014117821':'BETHA CHRISTY',
            '1015109216':'HIOE RINA MARIA',
            '1017098020':'ENVY ELISHA WIJAYA',
            '12113935':'SUGIYONO',
            '19041727':'KAISYA AZZAHRA KADAR SARIFANI'}

            results = pd.merge(results, results_alfabi, left_on='tbto_voucher_code', right_on='ELS_SMS_REGISTRATION_CODE', how='left')
            results['MSO_CREATE_USER_NAME'] = results['MSO_CREATE_USER'].map(create_user_map)
            results['MSO_CREATE_USER_NAME'] = results['MSO_CREATE_USER_NAME'].fillna('gli')

            results = pd.merge(results, rew_merge, left_on='tbto_voucher_code', right_on='reg_code', how='left')



        except Exception as e:
            print(e)
            pass



vcr_attach = '/home/server/gli-data-science/akhiyar/alfagift_alert/voucher_used_detail_{}.xlsx'.format(dr_order.strftime('%d%b%y'))
adder = 'alert'
writer = pd.ExcelWriter(vcr_attach, engine='xlsxwriter') 
out_voucher_check_2_save.to_excel(writer, sheet_name=adder, index=False)

# Auto-adjust columns' width
for column in out_voucher_check_2_save:
    column_width = max(out_voucher_check_2_save[column].astype(str).map(len).max(), len(column))
    col_idx = out_voucher_check_2_save.columns.get_loc(column)
    writer.sheets[adder].set_column(col_idx, col_idx, column_width)

adder = 'detail'
#     writer = pd.ExcelWriter(vcr_attach, engine='xlsxwriter') 
results.to_excel(writer, sheet_name=adder, index=False)

# Auto-adjust columns' width
for column in results:
    column_width = max(results[column].astype(str).map(len).max(), len(column))
    col_idx = results.columns.get_loc(column)
    writer.sheets[adder].set_column(col_idx, col_idx, column_width)

writer.save()

if (len(out_voucher_check) > 0) or (len(out_voucher_check_2) > 0):
    out_voucher_check['sum_voucher_usage'] = out_voucher_check['sum_voucher_usage']                            .astype(int).apply(lambda x : "{:,d}".format(x))
    out_voucher_check_2['count_unique_voucher'] = out_voucher_check_2['count_unique_voucher'].astype('int')
    m2 = True


# %%

# %%

# %%
### OLD WAY TO GET VOUCHER DETAIL

# out_voucher_check = ''
# out_voucher_check_2 = ''
# if b2:
#     q_2 = '''
#     -- check voucher amount usage and voucher code count in one day interval
#     select 
#         tbto_ponta_id, sum(tbto_voucher_usage) as sum_voucher_usage
#     from 
#         tb_transaction_order tto 
#     where 
#         tto.tbto_create_date between '{shift_str}' and '{now_str}' 
#         and tbto_ponta_id is not null
#         and tbto_voucher_usage is not null
#         and tto.tbto_status not in ('18','10','11')
#         and tbto_voucher_code not in ('')
#         and tbto_status is not null
#     group by tbto_ponta_id
#     order by sum_voucher_usage desc

#     '''.format(shift_str=str(dr_voucher-timedelta(days=1)), now_str=str(dr_voucher))
    
#     voucher_check = pd.read_sql(q_2, connection)
    
    
#     q_5 = '''
#     -- check voucher amount usage and voucher code count in one day interval
#     select 
#         tbto_ponta_id, count(distinct(tbto_voucher_code)) as count_unique_voucher
#     from 
#         tb_transaction_order tto 
#     where 
#         tto.tbto_create_date between '{shift_str}' and '{now_str}' 
#         and tbto_ponta_id is not null
#         and tbto_voucher_usage is not null
#         and tto.tbto_status not in ('18','10','11')
#         and tbto_voucher_code not in ('')
#         and tbto_status is not null
#     group by tbto_ponta_id
#     order by count_unique_voucher desc

#     '''.format(shift_str=str(dr_voucher-timedelta(days=1)), now_str=str(dr_voucher))
    
#     connection, cursor = ds_db.connect_prd_order_4()
#     voucher_check_2 = pd.read_sql(q_5, connection)
#     connection.close()
    
#     out_voucher_check = voucher_check[voucher_check['sum_voucher_usage'] >= const_out_voucher_amount]
#     out_voucher_check_2 = voucher_check_2[voucher_check_2['count_unique_voucher'] >= const_out_voucher_count]

    
#     if len(out_voucher_check_2) > 0:
#         if len(out_voucher_check_2) == 1:
#             tup_mem = tuple(list(out_voucher_check_2['tbto_ponta_id']) + ['1'])
#         else:
#             tup_mem = tuple(out_voucher_check_2['tbto_ponta_id'])
            
#         q='''
#         SELECT NO_KARTU, NAMA_LENGKAP, NIK_KARYAWAN
#         FROM MASTER_CUST mc 
#         WHERE NO_KARTU in {}

#         '''.format(tup_mem)
#         con = connect_alfabi()
#         mc = pd.read_sql(q, con)
#         con.close()

#         out_voucher_check_2_save = pd.merge(out_voucher_check_2, mc, left_on='tbto_ponta_id', 
#                                        right_on='NO_KARTU', how='left').fillna('-').drop('NO_KARTU',1)  
        
#         out_voucher_check_2_save = out_voucher_check_2_save.rename(columns={'tbto_ponta_id':'ponta_id', 
#                     'count_unique_voucher':'count', 'NAMA_LENGKAP':'nama', 'NIK_KARYAWAN':'NIK'})



#         ponta_id = tuple(map(str,list(out_voucher_check_2['tbto_ponta_id'])+['1']))

#         query = '''
#         select 
#             tto.tbto_no, tto.tbto_create_date, tto.tbto_ponta_id, tto.tbto_voucher_code, tto.tbto_voucher_usage
#         from 
#             tb_transaction_order tto 
#         where 
#             tto.tbto_create_date between '{shift_str}' and '{now_str}' 
#             and tbto_ponta_id is not null
#             and tbto_voucher_usage is not null
#             and tto.tbto_status not in ('18','10','11')
#             and tto.tbto_voucher_code not in ('')
#             and tto.tbto_status is not null
#             and tto.tbto_ponta_id in {ponta_id}
#         '''.format(shift_str=str(dr_voucher-timedelta(hours=30)), now_str=str(dr_voucher), ponta_id=ponta_id)
#         results = pd.read_sql_query(query, connection)
      
        
#         li_vcr = list(results['tbto_voucher_code'])

#         try:
#             ## find voucher in master voucher using voucher code to get reward id
#             koneksi4 = pymongo.MongoClient\
#             ("mongodb://user_read:read12345678@35.240.152.164:27017/alfagift_loyalty_promotion")
#             mydb = koneksi4["alfagift_loyalty_promotion"]
#             mycol = mydb["alfagift_master_voucher"]

            
#             rew_vcr = pd.DataFrame(mycol.find({'reg_code':{"$in":li_vcr}}))
#             rew_vcr = rew_vcr[['reward_id','reg_code','voucher_used_by','ponta_id']]
#             from bson.objectid import ObjectId
#             li_rew = [ObjectId(rew) for rew in list(rew_vcr['reward_id'])]
#             ##
            
#             ## find rewad in master reward to get detail campaign
#             koneksi4 = pymongo.MongoClient\
#                     ("mongodb://user_mkt:us3r_mkt@35.213.177.53:27017/alfagift_cms")
#             mydb = koneksi4["alfagift_cms"]
#             mycol = mydb["alfagift_master_reward"]


#             rew_det = pd.DataFrame(mycol.find({'_id':{"$in":li_rew}}))
#             rew_det = rew_det[['_id','reward_name','reward_start_date','reward_end_date','voucher_type'
#                                ,'created_at','created_by','created_by_mail']]
#             rew_det['_id'] = rew_det['_id'].astype(str)
#             ##
            
#             ## merging master voucher and campaign detail
#             rew_merge = pd.merge(rew_vcr,rew_det, left_on='reward_id', right_on='_id', how='left').drop('_id',1)


#         except Exception as e:
#             print(e)
#             rew_merge=pd.DataFrame()
#             pass
#         try:
#             if len(li_vcr) == 1:
#                 tup_vcr = tuple(li_vcr+['1'])
#             else:
#                 tup_vcr = tuple(li_vcr)
#             query = '''
#             SELECT est.ELS_RECEIPT, est.ELS_SMS_REGISTRATION_CODE, esmp.MSO_DESCP, esmp.MSO_KET, esmp.MSO_CREATE_USER 
#             FROM ELS_SMS_TRANS est 
#             LEFT JOIN ELS_SMS_MS_PROMO esmp 
#             ON est.ELS_MSO_ID  = esmp.MSO_ID 
#             WHERE est.ELS_SMS_REGISTRATION_CODE in {}


#             '''.format(tup_vcr)
#             con_alfabi = connect_alfabi()
#             results_alfabi = pd.read_sql_query(query, con_alfabi)


#             con_alfabi.close()

#             create_user_map={'1012115509':'SUGIYONO',
#             '1014117821':'BETHA CHRISTY',
#             '1015109216':'HIOE RINA MARIA',
#             '1017098020':'ENVY ELISHA WIJAYA',
#             '12113935':'SUGIYONO',
#             '19041727':'KAISYA AZZAHRA KADAR SARIFANI'}

#             results = pd.merge(results, results_alfabi, left_on='tbto_voucher_code', right_on='ELS_SMS_REGISTRATION_CODE', how='left')
#             results['MSO_CREATE_USER_NAME'] = results['MSO_CREATE_USER'].map(create_user_map)
#             results['MSO_CREATE_USER_NAME'] = results['MSO_CREATE_USER_NAME'].fillna('gli')
            
#             results = pd.merge(results, rew_merge, left_on='tbto_voucher_code', right_on='reg_code', how='left')



#         except Exception as e:
#             print(e)
#             pass



#     vcr_attach = '/home/server/gli-data-science/akhiyar/alfagift_alert/voucher_used_detail_{}.xlsx'.format(dr_order.strftime('%d%b%y'))
#     adder = 'alert'
#     writer = pd.ExcelWriter(vcr_attach, engine='xlsxwriter') 
#     out_voucher_check_2_save.to_excel(writer, sheet_name=adder, index=False)

#     # Auto-adjust columns' width
#     for column in out_voucher_check_2_save:
#         column_width = max(out_voucher_check_2_save[column].astype(str).map(len).max(), len(column))
#         col_idx = out_voucher_check_2_save.columns.get_loc(column)
#         writer.sheets[adder].set_column(col_idx, col_idx, column_width)
        
#     adder = 'detail'
# #     writer = pd.ExcelWriter(vcr_attach, engine='xlsxwriter') 
#     results.to_excel(writer, sheet_name=adder, index=False)

#     # Auto-adjust columns' width
#     for column in results:
#         column_width = max(results[column].astype(str).map(len).max(), len(column))
#         col_idx = results.columns.get_loc(column)
#         writer.sheets[adder].set_column(col_idx, col_idx, column_width)

#     writer.save()

#     if (len(out_voucher_check) > 0) or (len(out_voucher_check_2) > 0):
#         out_voucher_check['sum_voucher_usage'] = out_voucher_check['sum_voucher_usage']                            .astype(int).apply(lambda x : "{:,d}".format(x))
#         out_voucher_check_2['count_unique_voucher'] = out_voucher_check_2['count_unique_voucher'].astype('int')
#         m2 = True


# %%

# %%

# %%

# %%


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


# %%





# %%


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


# %%





# %%


print(m1,m2,m3,m4)


# %%





# %%
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
    preceiver = "product.operation@gli.id, william.d.sinolungan@gli.id, akhiyar.waladi@gli.id"

    #preceiver = "akhiyarwaladi@gmail.com"
    print(preceiver)


    psubject = 'Alfagift Alert [{}]'.format(email_date)
    pbody = """Time {} there is an abnormal transaction, please check below <br><hr><br> {}""".format(email_date, outdf_format)

    lib.kirim_email_noreply(preceiver, psubject, pbody, "")
    
    
    
    # telegram send message
    bot = telegram.Bot(token='1539145464:AAEZKQzDhEwir3x5PDLzYKHxLC-2Igc7Gyo')

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
        out_voucher_check_2 = out_voucher_check_2.rename(columns={'count_unique_voucher':'count'})[0:10]
    if len(out_point_redeem) > 0:
        out_point_redeem = out_point_redeem.rename(columns={'sum_ponta_redeem':'sum','count_order_id':'c'}).drop('c',1)
    if len(out_point_issue) > 0:
        out_point_issue = out_point_issue.rename(columns={'sum_ponta_issued':'sum','count_order_id':'c'}).drop('c',1)
    for idx, outdf in enumerate([out_count_order, out_voucher_check, out_voucher_check_2, out_point_redeem, out_point_issue]):
        if len(outdf) > 0:
            outdf_format += '{} {} \n\n<pre>{}</pre>\n\n------------------------------------------------------------------\n\n'            .format(email_date, body_format[idx], outdf.to_markdown(index=False, tablefmt="grid"))



    bot.send_message(chat_id='-1001309547292', text="{}".format(outdf_format), parse_mode=ParseMode.HTML)
    try:
        bot.sendDocument(chat_id='-1001309547292', document=open(vcr_attach, 'rb'))
    except Exception as e:
        print(e)
        pass


# %%





# %%





# %%
if b5:
    bot = telegram.Bot(token='1539145464:AAEZKQzDhEwir3x5PDLzYKHxLC-2Igc7Gyo')

    end_date = date.today()-timedelta(days=1)
    start_date = end_date.replace(day=1)

    c_attach = '/home/server/gli-data-science/akhiyar/data_req/used_release/used_release_{}_{}.xlsx'                .format(start_date.strftime("%d%b%y"),                end_date.strftime("%d%b%y")).format()
    c_head = '[Report] List Released Voucher in Success Order'
    c_body = 'Submit this file to database admin for manual intervention, so that voucher cannot be used repeatedly.'

    outdf_format = '{}\n\n{} \n\n------------------------------------------------------------------\n\n'    .format(c_head, c_body)

    ## mechanism to send email

    lib = lib_3d.desan()
    preceiver = "product.operation@gli.id, william.d.sinolungan@gli.id,                 akhiyar.waladi@gli.id"

    ## mechanism to send telegram

    lib.kirim_email_noreply(preceiver, c_head, c_body, c_attach)


    bot.send_message(chat_id='-1001309547292', text="{}".format(outdf_format),                     parse_mode=ParseMode.HTML)

    bot.sendDocument(chat_id='-1001309547292', document=open(c_attach, 'rb'))


# %%





# %%





# %%

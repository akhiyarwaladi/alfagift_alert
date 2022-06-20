#!/usr/bin/env python
# coding: utf-8
# %%
from datetime import datetime

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

from joblib import load

import numpy as np

import os

from gibberish_detector import detector

#from common import global_variable
# r_con = global_variable.r
import redis
r_con = redis.Redis(host='10.25.4.67',port='6379')


clf_path = './fraud_model/registrasi_logreg.pkl'
gib1_path = './fraud_model/indo_news.model'

with open('./fraud_model/all_email_provider_domains.txt') as f:
    li_all_provider = f.read().splitlines()

class RegisterCheck:
    def __init__(self) -> None:
        self.clf = load(clf_path)
        self.Detector = detector.create_from_model(
            gib1_path
        )

    def encode(self, s):

        i = 0
        t = ''
        while i < len(s):
            if s[i].isupper():
                t += 'U'
            elif s[i].islower():
                t += 'L'
            elif s[i].isdigit():
                t += 'D'
            else:
                t += 'O'
            i += 1

        return t

    def short_encode(self, s):
        i = 0
        s = self.encode(s)
        curr = ''
        t = ''

        while i < len(s):
            if curr != s[i]:
                t += s[i]
                curr = s[i]

            i += 1
        return t

    def score_sus_email(self, email_input):

        name_input = email_input.split('@')[0]
        provider_input = email_input.split('@')[-1]


        pat_count_digit = self.encode(name_input).count('D')
        pat_encode = self.short_encode(name_input)

        if pat_encode == 'LD' and pat_count_digit >= 4:
            return 1
        elif pat_encode == 'D':
            return 1
        elif provider_input not in li_all_provider:
            return 1
        return 0


        
    def score_gibberish(self, input_email):
        split_email = input_email.split('@')[0]

        score_gib = self.Detector.calculate_probability_of_being_gibberish(split_email)

        return score_gib
        
        
    def score_user_registrasi(self, email, phone, ip_addr, nama):
        
        is_fraud = False
        threshold_expire = 86400
        
        res_gibberish = self.score_gibberish(email)
        res_sus_email = self.score_sus_email(email)
        
        
        count_ip = r_con.get('regis_ip:{}'.format(ip_addr))
        if count_ip is not None:
            count_ip = int(count_ip.decode('utf-8'))
        else:
            count_ip = 0
            
        count_phone = r_con.get('regis_prefixphone:{}'.format(phone[0:9]))
        if count_phone is not None:
            count_phone = int(count_phone.decode('utf-8'))
        else:
            count_phone = 0
        

        li_input = [[
            res_gibberish,
            res_sus_email,
            count_ip,
            count_phone
        ]]
        X_test = np.array(li_input)

        li_param = [
        'score_gibberish',
        'score_sus_email',
        'createdFromIp_count',
        'phoneSub_count'
        ]


        dict_score_parameter = {}
        for idx in range(len(li_param)):
            dict_score_parameter[li_param[idx]] = self.clf.coef_[0][idx] * X_test[0][idx]
        
        score = self.clf.predict_proba(X_test)
        
        
        li_reason = []
        if score[0][-1] >= 0.6:
            is_fraud = True
            if res_gibberish == 1 or res_sus_email == 1:
                li_reason.append('Email kamu menggunakan pola tidak wajar')
            if count_ip >= 5:
                li_reason.append('IP kamu sudah digunakan oleh pendaftaran pada akun lain')
            if count_phone >= 5:
                li_reason.append('Prefix no handphone terlihat sama dengan pendaftar pada akun lain')
            
            
            
#         r_con.set('regis_ip:{}'.format(ip_addr),
#         count_ip + 1, ex=threshold_expire)

#         r_con.set('regis_prefixphone:{}'.format(phone[0:9]),
#         count_phone + 1, ex=threshold_expire)
        
        return {
            'is_fraud':is_fraud,
            'score_final':round(score[0][-1], 5),
            'score_parameter':dict_score_parameter,
            'reason':li_reason
        }
    
    def score_user_otp(self, ip_adress, device_id, phone_num, device_model):
        threshold_expire = 86400
        threshold_pair_ip_deviceid = 5
        threshold_pair_ip_devicemodel = 5
        threshold_pair_phone_deviceid = 5
        sum_score = 0
        score_pair1 = 0
        score_pair2 = 0
        score_pair3 = 0
        is_fraud = False
        
        cur_ip = ip_adress
        cur_device_id = device_id
        cur_prefix_phone = phone_num[0:9]
        cur_device_model = device_model.lower()
        
        pair_ip_deviceid = "{} - {}".format(cur_ip, cur_device_id)
        pair_ip_devicemodel = "{} - {}".format(cur_ip, cur_device_model)
        pair_phone_deviceid = "{} - {}".format(cur_prefix_phone, cur_device_id)
        
        print('pair 1 {}'.format(pair_ip_deviceid))
        print('pair 2 {}'.format(pair_ip_devicemodel))
        print('pair 3 {}'.format(pair_phone_deviceid))
        otp_ip_deviceid = r_con.get('otp_ip_deviceid:{}'.format(pair_ip_deviceid))
        otp_ip_devicemodel = r_con.get('otp_ip_devicemodel:{}'.format(pair_ip_devicemodel))
        otp_phone_deviceid = r_con.get('otp_phone_deviceid:{}'.format(pair_phone_deviceid))

        
        print('score 1 {}'.format(otp_ip_deviceid))
        print('score 2 {}'.format(otp_ip_devicemodel))
        print('score 3 {}'.format(otp_phone_deviceid))
        ######
        if otp_ip_deviceid is not None:
            otp_ip_deviceid = int(otp_ip_deviceid.decode('utf-8')) + 1

            if otp_ip_deviceid > threshold_pair_ip_deviceid:
                score_pair1 += (otp_ip_deviceid - threshold_pair_ip_deviceid) * 0.2

        else:
            otp_ip_deviceid = 1
            


        ######
        if otp_ip_devicemodel is not None:
            otp_ip_devicemodel = int(otp_ip_devicemodel.decode('utf-8')) + 1

            if otp_ip_devicemodel > threshold_pair_ip_devicemodel:
                score_pair2 += (otp_ip_devicemodel - threshold_pair_ip_devicemodel) * 0.2

        else:
            otp_ip_devicemodel = 1



        ######
        if otp_phone_deviceid is not None:
            otp_phone_deviceid = int(otp_phone_deviceid.decode('utf-8')) + 1

            if otp_phone_deviceid > threshold_pair_phone_deviceid:
                score_pair3 += (otp_phone_deviceid - threshold_pair_phone_deviceid) * 0.2

        else:
            otp_phone_deviceid = 1



#         r_con.set('otp_ip_deviceid:{}'.format(pair_ip_deviceid),
#               otp_ip_deviceid, ex=threshold_expire)

#         r_con.set('otp_ip_devicemodel:{}'.format(pair_ip_devicemodel),
#               otp_ip_devicemodel, ex=threshold_expire)

#         r_con.set('otp_phone_deviceid:{}'.format(pair_phone_deviceid),
#               otp_phone_deviceid, ex=threshold_expire)
        
        
        sum_score = score_pair1 + score_pair2 + score_pair3
        
        dict_score_parameter = {}
        dict_score_parameter['ip_deviceid'] = score_pair1
        dict_score_parameter['ip_devicemodel'] = score_pair2
        dict_score_parameter['phone_deviceid'] = score_pair3
        
        
        
        li_reason = []
        
        
        if sum_score > 0.6:
            is_fraud = True
            if score_pair1 > 0:
                li_reason.append('Unusual activity in your network')

        
        return {
            'is_fraud':is_fraud,
            'score_final':sum_score,
            'score_parameter':dict_score_parameter,
            'reason':li_reason
        }



# %%

# %%

# %%

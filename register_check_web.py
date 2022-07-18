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
        
        
    def score_user_registrasi_web(self, email, phone, ip_addr, nama):
        
        is_fraud = False
        threshold_expire = 86400
        
        res_gibberish = self.score_gibberish(email)
        res_sus_email = self.score_sus_email(email)
        
        
        count_ip = r_con.get('web_regis_ip:{}'.format(ip_addr))
        if count_ip is not None:
            count_ip = int(count_ip.decode('utf-8'))
        else:
            count_ip = 0
            
        count_phone = r_con.get('web_regis_prefixphone:{}'.format(phone[0:9]))
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
            if res_gibberish > 4 or res_sus_email == 1:
                li_reason.append('Email kamu menggunakan pola tidak wajar')
            if count_ip >= 1:
                li_reason.append('IP kamu sudah digunakan oleh pendaftaran pada akun lain')
            if count_phone >= 1:
                li_reason.append('Prefix no handphone terlihat sama dengan pendaftar pada akun lain')
            
            
            
        r_con.set('web_regis_ip:{}'.format(ip_addr),
        count_ip + 1, ex=threshold_expire)

        r_con.set('web_regis_prefixphone:{}'.format(phone[0:9]),
        count_phone + 1, ex=threshold_expire)
        
        return {
            'is_fraud':is_fraud,
            'score_final':round(score[0][-1], 5),
            'score_parameter':dict_score_parameter,
            'reason':li_reason
        }
    
    def score_user_otp_web(self, ip, fingerprint, phone_num, user_agent):
        threshold_expire = 86400
        
        threshold_pair_ip_fingerprint = 5
        threshold_pair_prefixphone_fingerprint = 5

        sum_score = 0
        score_pair1 = 0
        score_pair2 = 0
        is_fraud = False
        
        cur_ip = ip
        cur_fingerprint = fingerprint
        cur_prefixphone = phone_num[0:9]

        
        pair_ip_fingerprint = "{} - {}".format(cur_ip, cur_fingerprint)
        pair_prefixphone_fingerprint = "{} - {}".format(cur_prefixphone, cur_fingerprint)

        
        print('pair 1 {}'.format(pair_ip_fingerprint))
        print('pair 2 {}'.format(pair_prefixphone_fingerprint))

        otp_ip_fingerprint = r_con.get('web_otp_ip_fingerprint:{}'.format(pair_ip_fingerprint))
        otp_prefixphone_fingerprint = r_con.get('web_otp_prefixphone_fingerprint:{}'.format(pair_prefixphone_fingerprint))
    

        print('score 1 {}'.format(otp_ip_fingerprint))
        print('score 2 {}'.format(otp_prefixphone_fingerprint))

        ######
        if otp_ip_fingerprint is not None:
            otp_ip_fingerprint = int(otp_ip_fingerprint.decode('utf-8')) + 1

            if otp_ip_fingerprint > threshold_pair_ip_fingerprint:
                score_pair1 += (otp_ip_fingerprint - threshold_pair_ip_fingerprint) * 0.2

        else:
            otp_ip_fingerprint = 1
            


        ######
        if otp_prefixphone_fingerprint is not None:
            otp_prefixphone_fingerprint = int(otp_prefixphone_fingerprint.decode('utf-8')) + 1

            if otp_prefixphone_fingerprint > threshold_pair_prefixphone_fingerprint:
                score_pair2 += (otp_prefixphone_fingerprint - threshold_pair_prefixphone_fingerprint) * 0.2

        else:
            otp_prefixphone_fingerprint = 1



        r_con.set('web_otp_ip_fingerprint:{}'.format(pair_ip_fingerprint),
              otp_ip_fingerprint, ex=threshold_expire)

        r_con.set('web_otp_prefixphone_fingerprint:{}'.format(pair_prefixphone_fingerprint),
              otp_prefixphone_fingerprint, ex=threshold_expire)


        
        sum_score = score_pair1 + score_pair2
        
        dict_score_parameter = {}
        dict_score_parameter['ip_fingerprint'] = score_pair1
        dict_score_parameter['prefixphone_fingerprint'] = score_pair2

              
        li_reason = []
        
        
        if sum_score >= 0.4:
            is_fraud = True
            if score_pair1 > 0:
                li_reason.append('Pasangan IP dan Fingerprint kamu telah digunakan')
            if score_pair2 > 0:
                li_reason.append('Pasangan Phone Prefix and Fingerprint kamu telah digunakan')

        
        return {
            'is_fraud':is_fraud,
            'score_final':sum_score,
            'score_parameter':dict_score_parameter,
            'reason':li_reason
        }



# %%

# %%

# %%

# %%

# %%

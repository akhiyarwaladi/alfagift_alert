#!/usr/bin/env python
# coding: utf-8
# %%
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

from joblib import dump, load

from datetime import datetime, timedelta, date
import numpy as np

import os

from gibberish_detector import detector

import redis
r_con = redis.Redis(host="35.213.143.227", port=6379, db=0)

clf_path = './fraud_model/model/regis_logreg.pkl'
gib1_path = './fraud_model/gibberish-detector/indo_news.model'

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

    def score_sus_email(self, s):

        email_input = s
        email_input = email_input.split('@')[0]



        pat_count_digit = self.encode(email_input).count('D')
        pat_encode = self.short_encode(email_input)

        if pat_encode == 'LD' and pat_count_digit >= 4:
            return 1
        elif pat_encode == 'D':
            return 1
        return 0

        
        
    def score_gibberish(self, input_email):
        split_email = input_email.split('@')[0]

        flag_gibberish1 = False

        try:
            if self.Detector.is_gibberish(split_email):
                flag_gibberish1 = True
        except Exception as e:
            pass


        if flag_gibberish1:
            return 1
        else:
            return 0
        
        
    def score_user(self, email, phone, ip_addr, nama):
        
        is_fraud = False
        threshold_expire = 10
        
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
            
            
            
        r_con.set('regis_ip:{}'.format(ip_addr),
        count_ip + 1, ex=threshold_expire)

        r_con.set('regis_prefixphone:{}'.format(phone[0:9]),
        count_phone + 1, ex=threshold_expire)
        
        return {
            'is_fraud':is_fraud,
            'score_final':round(score[0][-1], 5),
            'score_parameter':dict_score_parameter,
            'reason':li_reason
        }


# %%

# %%

# %%

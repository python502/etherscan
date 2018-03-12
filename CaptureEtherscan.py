#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureEtherscan.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin
from bs4 import BeautifulSoup
from pyquery import PyQuery as jq
from MysqldbOperate import MysqldbOperate
import pandas as pd
import imp
import re
import time
import threadpool
import threading
import json
import execjs
import requests


MAXPOOL = 10
DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
class TimeoutException(Exception):
    def __init__(self, err='operation timed out'):
        super(TimeoutException, self).__init__(err)


class CaptureEtherscan(object):
    transactions = 0
    transactions_token = 1
    transfer_record_table = 'transfer_records'
    transfer_token_no = 'transfer_token_no'
    VAULE = 0
    def __init__(self, user_agent):
        self._rootID = ''
        self.s = requests.Session()
        # self.s.proxies = {"http": "socks5://127.0.0.1:9150", "https": "socks5://127.0.0.1:9150"}
        self.s.headers['user-agent'] = user_agent
        self.mysql = MysqldbOperate(DICT_MYSQL)
        self.transactions_infos = []
        self.transactions_token_infos = []
        self.transactions_token_error = []
        self.transactions_token_black = []
        self.insert_list = []
        self.lock = threading.Lock()
        self.lock1 = threading.Lock()
        self.flag_503 = False
        self.contract = '0xa9ec9f5c1547bd5b0247cf6ae3aab666d10948be'

    def __del__(self):
        if self.mysql:
            del self.mysql

    def __getNoOfTransactions(self, user_id):
        start_url = 'https://etherscan.io/address/{}'.format(user_id)
        page_source = self.getHtml(start_url)
        soup = BeautifulSoup(page_source, 'lxml')
        return int(soup.find('span', {'title': "Normal Transactions"}).getText().strip('\n').strip()[:-4].strip())


    def __geterc20contract(self, user_id):
        try:
            start_url = 'https://etherscan.io/address/{}'.format(user_id)
            page_source = self.getHtml(start_url)
            soup = BeautifulSoup(page_source, 'lxml')
            datas = soup.find('ul', {'id': "balancelist", 'class':"dropdown-menu"}).findAll('a')
            for data in datas:
                if data.find('i', {'class': "liH"}).getText().strip() == 'SAY':
                    self.contract = data.find('span', {'class':"address-tag"}).getText().strip()
        except Exception, e:
            logger.debug('__geterc20contract user_id:{} error:{}'.format(user_id, e))
        finally:
            return self.contract

    @property
    def rootId(self):
        return self._rootID

    @rootId.setter
    def rootId(self, value):
        self._rootID = value

    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def __getNoOfTransactionsToken(self, token_url):
        try:
            page_source = self.getHtml(token_url)
            pattern = re.compile(r'var totaltxns = \'\d+\'', re.M)
            totaltxns = pattern.findall(page_source)[0]
            pattern = re.compile(r'\d+', re.M)
            totaltxns = int(pattern.findall(totaltxns)[0])
            return totaltxns
        except Exception, e:
            # logger.error('__getNoOfTransactionsToken error:{}'.format(e))
            raise ValueError('token_url{} getNoOfTransactionsToken error:{}'.format(token_url, e))

    # def _rm_duplicate(self, scr_datas, match):
    #     key_value = []
    #     result = []
    #     for data in scr_datas:
    #         if data.get(match) in key_value:
    #             logger.debug('find repead data: {}'.format(data))
    #             continue
    #         else:
    #             key_value.append(data.get(match))
    #         result.append(data)
    #     return result
    def _rm_duplicate(self, scr_datas, match):
        data = pd.DataFrame(scr_datas)
        # 去重
        data = data.drop_duplicates([match])
        return data.to_dict(orient='records')

    def save_result_id(self,requests,result):
        if result and self.lock.acquire():
            self.transactions_infos.extend(result)
            self.lock.release()

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def getHtml(self, url):
        resp = self.s.get(url)
        netcode = resp.status_code
        try:
            # if self.resp_check(resp):
            #     return resp.text
            if netcode == 200:
                return resp.text
            elif netcode == 302:
                return resp.text
            elif netcode == 503:
                self.lock.acquire()
                dict(self.s.cookies.items())['cf_clearance'] = None
                for i in range(3):
                    self.getcookie(resp)
                    if dict(self.s.cookies.items())['cf_clearance']:
                        # logger.info('503 error and get cookie:{}'.format(dict(self.s.cookies.items()).get('cf_clearance')))
                        break
                else:
                    self.lock.release()
                    raise ValueError('get cf_clearance error')
                self.lock.release()
                return self.s.get(url).text
        except Exception, e:
            logger.error('e:{}'.format(e))
            raise

    def resp_check(self, resp):
        netcode = resp.status_code
        if netcode == 200:
            return True
        elif netcode == 302:
            return True
        elif netcode == 503:
            self.lock.acquire()
            if not dict(self.s.cookies.items()).get('cf_clearance'):
                try:
                    self.getcookie(resp)
                    # self.lock.release()
                    raise ValueError('503 error and get cookie')
                except Exception:
                    self.lock.release()
                    raise
            else:
                self.lock.release()
                raise ValueError('503 error and find cookie')
        else:
            # logger.error(netcode)
            # logger.error(self.s.cookies)
            raise ValueError('netcode: {} {}'.format(netcode, resp.reason))
    # @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def getcookie(self, resp, timeout=1):
        if not dict(self.s.cookies.items()).get('cf_clearance'):
            self.s.get(urljoin(resp.url, "/404"))
            # print urljoin(resp.url, "/favicon.ico")
            self.s.get(urljoin(resp.url, "/favicon.ico"))
            cookie_url = self.data_parser(resp)
            # self.s.headers["Referer"] = resp.url
            # print(cookie_url, resp.url)
            time.sleep(5)
            resp = self.s.get(cookie_url)
            startTime = datetime.now()
            endTime = datetime.now()
            while (endTime - startTime).seconds < timeout:
                cf = dict(self.s.cookies.items()).get('cf_clearance')
                if cf:
                    logger.info('get cookie cf_clearance: {}'.format(cf))
                    break
                endTime = datetime.now()
            else:
                raise TimeoutException('get cookie timeour error')
            # print(resp.headers)
        # return self.getHtml(resp.url)
        
    def data_parser(self, resp):
        content = resp.text
        jqdata = jq(content)("#challenge-form")
        path = jqdata.attr("action")
        scripts = jq(content)("script").eq(0).text()
        jscode = scripts.replace("\n", "").split(
            "setTimeout(function(){")[1].split("f.action += location.hash")[0].replace("a.value", "res").replace("t.length", "12").split(";")
        keydata = json.loads('{"' + jscode[0].split(",")[-1].replace(
            '":', '":"').replace("}", '"}').replace("=", '":') + "}")
        key = list(keydata.keys())[0]
        key_2 = list(keydata[key].keys())[0]
        key_3 = "{key}.{key_2}".format(key=key, key_2=key_2).strip()

        start_data = execjs.eval(jscode[0].split(",")[-1])
        logger.debug("keycode_{0} -- > {1}".format(start_data[key_2], jscode[0].split(',')[-1]))
        for i in jscode[10:-3]:
            code = i.replace(key_3, "")
            method = code[0]
            code = "{" + '"{0}":{1}'.format(key_2, code[2:]) + "}"
            start_data[key_2] = eval(
                "start_data[key_2]{0}{1}".format(method, execjs.eval(code)[key_2]))
            logger.debug("keycode_{0} -- > {1}".format(start_data[key_2], code))

        params = {i.attr("name"): i.attr("value")
                  for i in jqdata('input').items()}
        params["jschl_answer"] = execjs.eval(
            jscode[-3].replace(key_3, str(start_data[key_2])))
        return '{0}?{1}'.format(urljoin(resp.url, path), "&".join(["{0}={1}".format(k, v) for k, v in params.items()]))

    '''
    function: 
    @return: True or raise
    '''
    def dealTransactions(self, user_id):
        try:
            trans_actions = self.getTransactionsInfos(user_id)
            format_select = 'SELECT ID FROM {} WHERE TxHash="{{txhash}}" AND name="{{name}}" ORDER BY CREATE_TIME DESC'
            good_datas = trans_actions
            select_sql = format_select.format(self.transfer_record_table)
            table = self.transfer_record_table
            replace_insert_columns = ['name', 'TxHash', 'Block', 'From_account', 'To_account', 'Value', 'TxFee', 'create_time', 'operate_type', 'source_type','token']
            return self.save_recodes(good_datas, table, select_sql, replace_insert_columns)
        except Exception, e:
            logger.error('dealTransactions infos error:{},retry it'.format(e))
            raise

    def getTransactionsInfos(self, user_id):
        try:
            formatUrl = 'https://etherscan.io/txs?a={}&p={}'
            num = self.__getNoOfTransactions(user_id)
            if num % 50 == 0:
                page = num / 50
            else:
                page = num / 50 + 1
            pool_num = min(page, MAXPOOL)
            page_info = []
            logger.info('page num: {}'.format(page))
            page = min(page, 2000)
            for x in range(page):
                x += 1
                url = formatUrl.format(user_id, x)
                page_info.append([url, user_id])
            pool = threadpool.ThreadPool(pool_num)
            requests = threadpool.makeRequests(self.getTransactionsInfo, page_info, self.save_result_id)
            [pool.putRequest(req) for req in requests]
            pool.wait()
            pool.dismissWorkers(pool_num)

            logger.info('getTransactionsInfos get data: {}'.format(len(self.transactions_infos)))
            return self.transactions_infos
        except Exception, e:
            logger.error('getTransactionsInfos error:{},retry it'.format(e))
            raise

    @retry(stop_max_attempt_number=20, wait_fixed=5000)
    def getTransactionsInfo(self, page_info):
        try:
            logger.info('getTransactionsInfo url: {} begin'.format(page_info[0]))
            result_datas = []
            page_source = self.getHtml(page_info[0])
            soup = BeautifulSoup(page_source, 'lxml')
            datas = soup.find('table', {'class': "table table-hover "}).find('tbody').findAll('tr')
            for sourceData in datas:
                infos = sourceData.findAll('td')
                if len(infos) != 8:
                    break
                resultData = {}
                if infos[0].find('font'):
                    continue
                resultData['name'] = page_info[1]
                resultData['TxHash'.lower()] = infos[0].find('span').getText().strip()
                resultData['Block'.lower()] = int(infos[1].getText().strip())
                # resultData['From_account'.lower()] = infos[3].find('span').getText().strip()
                # try:
                #     resultData['To_account'.lower()] = infos[5].find('span').getText().strip()
                # except AttributeError,e:
                #     resultData['To_account'.lower()] = infos[5].getText().strip()
                # except Exception,e:
                #     logger.error('e:{}'.format(e))
                #     resultData['To_account'.lower()] = 'error'
                try:
                    From_account = infos[3].find('a').attrs['href']
                    resultData['From_account'.lower()] = From_account[9:-10]
                except AttributeError,e:
                    resultData['From_account'.lower()] = infos[3].getText().strip()
                except Exception,e:
                    logger.error('e:{}'.format(e))
                    resultData['From_account'.lower()] = 'error'

                try:
                    To_account = infos[5].find('a').attrs['href']
                    resultData['To_account'.lower()] = To_account[9:-10]
                except AttributeError,e:
                    resultData['To_account'.lower()] = infos[5].getText().strip()
                except Exception,e:
                    logger.error('e:{}'.format(e))
                    resultData['To_account'.lower()] = 'error'

                value = ''.join(infos[6].getText().split(','))
                resultData['Value'.lower()] = float(value[:-5].strip() if value.find('Ether') !=-1 else value.strip())
                TxFee = ''.join(infos[7].getText().split(','))
                resultData['TxFee'.lower()] = float(TxFee[:-5].strip() if TxFee.find('Ether') !=-1 else TxFee.strip())

                source_time = infos[2].find('span').attrs['title']
                #https://zhidao.baidu.com/question/518825268153206565.html
                imp.acquire_lock()
                resultData['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(source_time, '%b-%d-%Y %X %p'))
                imp.release_lock()

                resultData['operate_type'] = infos[4].find('span').getText().strip()
                resultData['source_type'] = self.transactions
                resultData['token'] = page_info[0]
                result_datas.append(resultData)
            logger.info('getTransactionsInfo url: {} done'.format(page_info[0]))
            return result_datas
        except Exception, e:
            # logger.error('getTransactionsInfo error: {}'.format(e))
            logger.error('url error: {}'.format(page_info[0]))
            raise
    '''
    function: 
    @return: True or raise
    '''
    # def dealTransactionsToken(self, user_id):
    #     try:
    #         trans_actions = self.getTransactionsTokenInfos(user_id)
    #         if not trans_actions:
    #             return []
    #         format_select = 'SELECT ID FROM {} WHERE TxHash="{{txhash}}" AND name="{{name}}" ORDER BY CREATE_TIME DESC'
    #         good_datas = trans_actions
    #         select_sql = format_select.format(self.transfer_record_table)
    #         table = self.transfer_record_table
    #         replace_insert_columns = ['name', 'TxHash', 'From_account', 'To_account', 'Value','create_time', 'operate_type', 'source_type', 'Token']
    #         # return self.save_recodes(good_datas, table, select_sql, replace_insert_columns)
    #         return self.save_recodes(good_datas, table, select_sql, replace_insert_columns) and trans_actions
    #     except Exception, e:
    #         logger.error('dealTransactionsToken infos error:{},retry it'.format(e))
    #         raise

    def getTransactionsTokentxns(self, user_id):
        try:
            url = 'https://etherscan.io/address-tokenpage?a={}'.format(user_id)
            logger.info('getTransactionsTokentxns url: {} begin'.format(url))
            result_datas = []
            page_source = self.getHtml(url)
            soup = BeautifulSoup(page_source, 'lxml')
            datas = soup.find('table', {'class': "table"}).findAll('tr')[1:]
            for sourceData in datas:
                resultData = {}
                infos = sourceData.findAll('td')
                if len(infos) != 7:
                    break

                token = infos[6].getText().strip()
                resultData['Token'.lower()] = token if token.find('Erc20') == -1 else token[7:-1]
                resultData['operate_type'] = infos[3].find('span').getText().strip()
                resultData['Value'.lower()] = float(infos[5].getText().replace(',', ''))
                if resultData['Token'.lower()] != 'SAY' or resultData['operate_type'] != 'OUT' or resultData['Value'.lower()] < self.VAULE:
                    continue
                resultData['name'] = user_id
                resultData['TxHash'.lower()] = infos[0].find('span').getText().strip()
                try:
                    From_account = infos[2].find('a').attrs['href']
                    resultData['From_account'.lower()] = From_account[9:-10]
                except AttributeError, e:
                    resultData['From_account'.lower()] = infos[2].getText().strip()
                except Exception, e:
                    self.logger.error('e:{}'.format(e))
                    resultData['From_account'.lower()] = 'error'

                try:
                    To_account = infos[4].find('a').attrs['href']
                    resultData['To_account'.lower()] = To_account[9:-10]
                except AttributeError, e:
                    resultData['To_account'.lower()] = infos[4].getText().strip()
                except Exception, e:
                    self.logger.error('e:{}'.format(e))
                    resultData['To_account'.lower()] = 'error'

                source_time = infos[1].find('span').attrs['title']
                resultData['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S",time.strptime(source_time, '%b-%d-%Y %X %p'))

                resultData['source_type'] = self.transactions_token
                result_datas.append(resultData)
            logger.info('all get data: {}'.format(len(result_datas)))
            return result_datas
        except Exception, e:
            logger.error('getTransactionsTokentxns url: {} error:{}'.format(url, e))
            raise

    def check_recodes(self, select_sql, sourcedatas):
        insert_datas=[]
        for sourcedata in sourcedatas:
            try:
                sql = select_sql.format(**sourcedata)
                logger.debug('select sql: {}'.format(sql))
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    continue
            except Exception, e:
                logger.error('check_recodes\'s error: {}.'.format(e))
                logger.error('check_recodes\'s sql: {}.'.format(sql))
                continue
        return insert_datas

    def save_recodes(self, good_datas, table, select_sql, insert_columns):
        try:
            result_insert = True
            if not good_datas:
                # logger.error('saveDatas not get datas')
                return True
            insert_datas = self.check_recodes(select_sql, good_datas)
            if insert_datas:
                operate_type = 'insert'
                l = len(insert_datas)
                logger.debug('len insert_datas: {}'.format(l))
                result_insert = self.mysql.insert_batch(operate_type, table, insert_columns, insert_datas)
                logger.debug('save_recodes result_insert: {}'.format(result_insert))
            return result_insert
        except Exception, e:
            logger.error('save_recodes error: {}.'.format(e))
            return False


    def dealTransactionsTokenList(self, user_id):
        trans_actions = self.dealTransactionsToken(user_id)
        if trans_actions is False:
            logger.error('user_id:{} done and unqualified'.format(user_id))
            self.transactions_token_error.append(user_id)
        else:
            self.insert_list.append(user_id)
            logger.info('user_id: {} done'.format(user_id))
            num = 0
            for trans_action in trans_actions:
                To_account = trans_action.get('To_account'.lower())
                # if trans_action.get('Token'.lower()) == 'SAY' and trans_action.get('operate_type'.lower()) == 'OUT' and  To_account not in self.insert_list:
                if trans_action.get('operate_type'.lower()) == 'OUT':
                    num+=1
                    if To_account not in self.insert_list and To_account not in self.transactions_token_error and To_account not in self.transactions_token_black:
                        self.dealTransactionsTokenList(To_account)
                # if trans_action.get('operate_type'.lower()) == 'OUT' and To_account not in self.insert_list and To_account not in self.transactions_token_error:
                #     num+=1
                #     self.dealTransactionsTokenList(To_account)
            logger.info('user_id: {} have out num:{}'.format(user_id, num))



    def save_result_token(self,requests,result):
        if result and self.lock.acquire():
            self.transactions_token_infos.extend(result)
            self.lock.release()

    '''
    function: 
    @return: True or raise
    '''
    def dealTransactionsToken(self, user_id):
        try:
            self.transactions_token_infos = []
            trans_actions = self.getTransactionsTokenInfos(user_id)
            trans_actions = self._rm_duplicate(trans_actions, 'TxHash'.lower())
            logger.info('dealTransactionsToken get data: {}'.format(len(trans_actions)))
            format_select = 'SELECT ID FROM {} WHERE TxHash="{{txhash}}" AND name="{{name}}" ORDER BY CREATE_TIME DESC'
            good_datas = trans_actions
            select_sql = format_select.format(self.transfer_record_table)
            table = self.transfer_record_table
            replace_insert_columns = ['name', 'TxHash', 'From_account', 'To_account', 'Value', 'create_time', 'operate_type', 'source_type', 'token', 'root']
            return self.save_recodes(good_datas, table, select_sql, replace_insert_columns) and trans_actions
        except Exception, e:
            logger.error('dealTransactionsToken user_id:{} error:{}'.format(user_id, e))
            return False




    def getTransactionsTokenInfos(self, user_id):
        try:
            # try:
            #     erc20contract = self.__geterc20contract(user_id)
            # except AttributeError:
            #     self.transactions_token_infos = self.getTransactionsTokentxns(user_id)
            # else:
            erc20contract = self.__geterc20contract(user_id)
            token_url = 'https://etherscan.io/token/generic-tokentxns2?contractAddress={}&a={}&mode='.format(erc20contract, user_id)
            num = self.__getNoOfTransactionsToken(token_url)
            if num % 50 == 0:
                page = num / 50
            else:
                page = num / 50 + 1
            pool_num = min(page, MAXPOOL)
            page_info = []
            logger.debug('page num: {}'.format(page))
            page = min(page, 2000)
            format_url = 'https://etherscan.io/token/generic-tokentxns2?contractAddress={}&mode=&a={}&p={}'
            for x in range(page):
                x += 1
                url = format_url.format(erc20contract, user_id, x)
                page_info.append([url, user_id])
            pool = threadpool.ThreadPool(pool_num)
            requests = threadpool.makeRequests(self.getTransactionsTokenInfo, page_info, self.save_result_token)
            [pool.putRequest(req) for req in requests]
            pool.wait()
            pool.dismissWorkers(pool_num)

            logger.info('getTransactionsTokenInfos get data: {}'.format(len(self.transactions_token_infos)))
            return self.transactions_token_infos
        except Exception, e:
            logger.error('getTransactionsTokenInfos error:{}'.format(e))
            raise

    @retry(stop_max_attempt_number=100, wait_fixed=5000)
    def getTransactionsTokenInfo(self, page_info):
        try:
            logger.info('getTransactionsTokenInfo url: {} begin'.format(page_info[0]))
            result_datas = []
            page_source = self.getHtml(page_info[0])
            soup = BeautifulSoup(page_source, 'lxml')
            datas = soup.find('table', {'class': "table"}).findAll('tr')[1:]
            for sourceData in datas:
                infos = sourceData.findAll('td')
                if len(infos) != 7:
                    break
                resultData = {}
                resultData['operate_type'] = infos[3].find('span').getText().strip()
                resultData['Value'.lower()] = float(infos[5].getText().replace(',', ''))

                if resultData['operate_type'] != 'OUT' or resultData['Value'.lower()] < self.VAULE:
                    continue
                resultData['name'] = page_info[1]
                resultData['TxHash'.lower()] = infos[0].find('span').getText().strip()

                try:
                    From_account = infos[2].find('a').attrs['href']
                    pattern = re.compile('a=[\w]*', re.M)
                    From_account = pattern.findall(From_account)[0]
                    resultData['From_account'.lower()] = From_account[2:]
                except AttributeError,e:
                    resultData['From_account'.lower()] = infos[2].getText().strip()
                except Exception,e:
                    logger.error('e:{}'.format(e))
                    resultData['From_account'.lower()] = 'error'

                try:
                    To_account = infos[4].find('a').attrs['href']
                    pattern = re.compile('a=[\w]*', re.M)
                    To_account = pattern.findall(To_account)[0]
                    resultData['To_account'.lower()] = To_account[2:]
                except AttributeError,e:
                    resultData['To_account'.lower()] = infos[4].getText().strip()
                except Exception,e:
                    logger.error('e:{}'.format(e))
                    resultData['To_account'.lower()] = 'error'

                source_time = infos[1].find('span').attrs['title']
                #https://zhidao.baidu.com/question/518825268153206565.html
                imp.acquire_lock()
                resultData['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(source_time, '%b-%d-%Y %X %p'))
                imp.release_lock()
                # a= int(time.strftime("%Y%m%d%H%M%S", time.strptime(source_time, '%b-%d-%Y %X %p')))
                # if a <= 20180308000000:
                #     continue

                resultData['source_type'] = self.transactions_token
                resultData['token'] = page_info[0]
                resultData['root'] = self.rootId
                result_datas.append(resultData)
            logger.info('getTransactionsTokenInfo url: {} done'.format(page_info[0]))
            return result_datas
        except AttributeError, e:
            pattern = re.compile(r'You have reached your maximum request limit for this resource')
            if pattern.search(page_source):
                logger.debug('url maximum request limit error: {}'.format(page_info[0]))
            else:
                logger.debug('url: {} error: {}'.format(page_info[0], e))
            raise
        except Exception, e:
            # logger.error('getTransactionsInfo error: {}'.format(e))
            logger.debug('url error: {}'.format(page_info[0]))
            raise

    def captureTransactionsToken(self, user_id):
        self.dealTransactionsTokenList(user_id)
        while self.transactions_token_error:
            tmp_id = self.transactions_token_error
            logger.info('* '*40)
            logger.info(self.transactions_token_error)
            logger.info(len(self.transactions_token_error))
            logger.info('* '*40)
            for id in tmp_id:
                logger.info('error id: {} repeat capture'.format(id))
                self.dealTransactionsTokenList(id)
                self.transactions_token_error.remove(id)
        logger.info('# '*40)
        logger.info(self.insert_list)
        logger.info(len(self.insert_list))
        logger.info('# '*40)

    def save_token_num(self,good_datas, table, replace_columns):
        try:
            result_replace = True
            if not good_datas:
                # logger.error('saveDatas not get datas')
                return True
            if good_datas:
                operate_type = 'replace'
                result_replace = self.mysql.insert_batch(operate_type, table, replace_columns, good_datas)
                logger.info('save_token_num result_replace: {}'.format(result_replace))
            return result_replace
        except Exception, e:
            logger.error('save_recodes error: {}.'.format(e))
            return False
    def recordCount(self,trans_actions):
        if not trans_actions:
            return []
        from_account = trans_actions[0].get('from_account')
        to_accounts = [i.get('to_account') for i in trans_actions]
        set_to_accounts = set(to_accounts)
        records = []
        for item in set_to_accounts:
            record = {}
            record['count'] = to_accounts.count(item)
            record['from_account'] = from_account
            record['to_account'] = item
            records.append(record)
        return records

    def dealTransactionsTokenNo(self, user_id):
        try:
            self.transactions_token_infos = []
            trans_actions = self.getTransactionsTokenInfos(user_id)
            trans_actions = self._rm_duplicate(trans_actions, 'TxHash'.lower())
            logger.info('dealTransactionsToken get data: {}'.format(len(trans_actions)))
            good_datas = self.recordCount(trans_actions)
            table = self.transfer_token_no
            replace_insert_columns = ['From_account', 'To_account', 'count']
            return self.save_token_num(good_datas, table, replace_insert_columns) and trans_actions
        except Exception, e:
            logger.error('dealTransactionsToken user_id:{} error:{}'.format(user_id, e))
            return False
def main():
    startTime = datetime.now()
    useragent = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Mobile Safari/537.36'
    objCaptureEtherscan = CaptureEtherscan(useragent)
    # urer_id = '0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1'
    # objCaptureEtherscan.rootId = urer_id
    # objCaptureEtherscan.captureTransactionsToken(urer_id)


    # objCaptureEtherscan.dealTransactions('0xf726dc178d1a4d9292a8d63f01e0fa0a1235e65c')
    # objCaptureEtherscan.dealTransactionsTokenList('0xa6c5427ca28364edcd61f781311b645e82775016')
    # logger.info(objCaptureEtherscan.insert_list)
    # logger.info(len(objCaptureEtherscan.insert_list))
    # logger.info(objCaptureEtherscan.transactions_token_error)
    # logger.info(len(objCaptureEtherscan.transactions_token_error))
    # objCaptureEtherscan.dealTransactionsToken('0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1')
    objCaptureEtherscan.dealTransactionsTokenNo('0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1')
    # objCaptureEtherscan.getTransactionsTokenInfo(['https://etherscan.io/token/generic-tokentxns2?contractAddress=0xa9ec9f5c1547bd5b0247cf6ae3aab666d10948be&mode=&a=0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1&p=2','0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1'])
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()


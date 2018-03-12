import execjs
import requests
import os
import logging
from pyquery import PyQuery as jq
from urlparse import urljoin
import json
import re
import time


logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] >>> %(levelname)s  %(name)s: %(message)s")


class bot(object):
    """docstring for bot"""

    def __init__(self):
        super(bot, self).__init__()
        self.loger = logging.getLogger(type(self).__name__)
        self.main_url = "https://etherscan.io/token/generic-tokentxns2?contractAddress=0xa9ec9f5c1547bd5b0247cf6ae3aab666d10948be&mode=&a=0x2f70fab04c0b4aa88af11304ea1ebfcc851c75d1&p={}"
        self.s = requests.Session()
        self.datas = {}
        # self.s.headers = {"Connection": "close", "Cache-Control": "max-age=0", "Upgrade-Insecure-Requests": "1",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"}

    def resp_check(self, resp):
        netcode = resp.status_code
        # self.s.headers["Referer"] = resp.url
        if netcode == 200:
            return True
        elif netcode == 302:
            return True
        else:
            self.loger.error(netcode)
            self.loger.error(self.s.cookies)
            self.getcookie(resp)

    def data_parser(self, resp, method=None):
        if method == "getcookie":
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
            self.loger.info(
                "keycode_{0} -- > {1}".format(start_data[key_2], jscode[0].split(',')[-1]))
            for i in jscode[10:-3]:
                code = i.replace(key_3, "")
                method = code[0]
                code = "{" + '"{0}":{1}'.format(key_2, code[2:]) + "}"
                start_data[key_2] = eval(
                    "start_data[key_2]{0}{1}".format(method, execjs.eval(code)[key_2]))
                self.loger.info("keycode_{0} -- > {1}".format(start_data[key_2], code))

            params = {i.attr("name"): i.attr("value")
                      for i in jqdata('input').items()}
            params["jschl_answer"] = execjs.eval(
                jscode[-3].replace(key_3, str(start_data[key_2])))
            return '{0}?{1}'.format(urljoin(self.main_url, path), "&".join(["{0}={1}".format(k, v) for k,v in params.items()]))

        else:
            with open("test.html", "wb") as f:
                f.write(resp.content)
            for item in jq(resp.text)("#maindiv >table tr").items():
                key = item("td:nth-child(1)").text()
                self.datas[key] = {}
                self.datas[key]['TxHASH'] = item("td:nth-child(1)").text()
                self.datas[key]['Age'] = item("td:nth-child(2)").text()
                self.datas[key]['From'] = item("td:nth-child(3)").text()
                self.datas[key]['Other'] = item("td:nth-child(4)").text()
                self.datas[key]['To'] = item("td:nth-child(5)").text()
                self.datas[key]['Quantity'] = item("td:nth-child(5)").text()

    def getcookie(self, resp):
        self.s.get(urljoin(self.main_url, "/404"))
        print urljoin(self.main_url, "/favicon.ico")
        self.s.get(urljoin(self.main_url, "/favicon.ico"))
        cookie_url = self.data_parser(resp, "getcookie")
        print(cookie_url, resp.url)
        time.sleep(5)
        # self.s.headers["Referer"] = resp.url
        resp = self.s.get(cookie_url)

        print(resp.headers)
        self.getpicitem(url=resp.url)

    def getpicitem(self, page=1, url=None):
        url = url if url else self.main_url.format(page)
        resp = self.s.get(url)
        if self.resp_check(resp):
            self.data_parser(resp)

    def start(self):
        for i in range(1, 10):
            self.getpicitem(page=i)
        with open("data.json", "w") as f:
            f.write(json.dumps(self.datas))


if __name__ == '__main__':
    spider = bot()
    spider.start()

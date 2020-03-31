#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/03/29 13:04
# @Author  : lizhe
# @Site    :
# @File    : crawler.py
# @Software: PyCharm

# IP地址取自国内髙匿代理IP网站：http://www.xicidaili.com/nn/
# 仅仅爬取首页IP地址就足够一般使用
import schedule
import datetime
import dateutil
import requests
import pandas as pd
import re
import time


def getContractNum(date):
    year = date.year
    month = date.month
    day = date.day
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    if day < 10:
        day = '0' + str(day)
    else:
        day = str(day)
    year = str(year)
    result = year + month + day
    return result


def date2str(date):
    # 输入日期
    # 返回时间字符串
    # 如果是周六日则返回字符串'0'
    year = date.year
    month = date.month
    day = date.day
    week = date.weekday() + 1
    if week == 6 or week == 7:
        return '0'
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    if day < 10:
        day = '0' + str(day)
    else:
        day = str(day)
    year = str(year)
    result = year + month + day
    return result


def crawler_futures(url, pattern):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    html = cla(url, headers)

    content = html.content.decode('utf-8')
    result = re.findall(pattern, content, re.DOTALL)
    return result


def cla(url, headers):
    try:
        html = requests.get(url=url, headers=headers, verify=False, timeout=1)
    except Exception:
        print("K.O.   ...restart")
        # time.sleep(random.randint(1, 10))
        html = cla(url, headers)
    return html


def crawler_one_day(delta, contract):
    # 查询期货合约编号contract
    # 从今天起查询delta天之前的数据
    pattern = 'PRODUCTNAME.*?铝.*?DELIVERYMONTH.*?' + contract + '.*?"SETTLEMENTPRICE.*?(\d+).*PRODUCTNAME.*?锌'
    date = datetime.datetime.now() - datetime.timedelta(days=delta)
    dateString = date2str(date)
    url = 'http://www.shfe.com.cn/data/dailydata/kx/kx{datetime}.dat'
    if dateString != '0':
        url = url.format(datetime=dateString)
        res = crawler_futures(url, pattern)
        if not res:
            return 0
        data = {
            'contract': contract,
            'fdate': dateString[:4] + '-' + dateString[4:6] + '-' + dateString[6:8],
            'settlementprice': res[0],
            'scrapy_time': datetime.datetime.now()
        }
        return data
    else:
        return 0


def main(afterMonths, beforDays):
    res = []
    today = datetime.datetime.now()
    for contractNumber in range(afterMonths, afterMonths+1):
        # 爬取未来月份的合约
        future = today + dateutil.relativedelta.relativedelta(months=contractNumber)
        futureContract = getContractNum(future)
        print('正在爬取期货合约号：' + futureContract[2:6])
        for i in range(0, beforDays+1):
            print('days:' +str(i))
            result = crawler_one_day(i, futureContract[2:6])
            if result != 0:
                res.append(result)
    pdResult = pd.DataFrame(res)
    pdResult.to_csv('上海期货-铝.csv', index=False, header=False, mode='a')


def job():
    print("I'm working...")
    main(3, 0)

schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)


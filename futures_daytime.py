#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/03/29 13:04
# @Author  : lizhe
# @Site    :
# @File    : crawler.py
# @Software: PyCharm

# IP地址取自国内髙匿代理IP网站：http://www.xicidaili.com/nn/
# 仅仅爬取首页IP地址就足够一般使用
# import schedule
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


def crawler_one_day(date):
    # date：要爬取的日期
    # 爬取合约根据当前日期而定，如果日期为15之前（含15日），则第一条合约为本月合约，如果超过15，则爬取下一月合约
    # 目前是爬取前五行数据
    result_ls = []
    dateString = date2str(date)
    if  dateString != '0':
        url = 'http://www.shfe.com.cn/data/dailydata/kx/kx{datetime}.dat'
        url = url.format(datetime=dateString)
        # 由于周日周六如果是15日，那么合约交割更新日期顺延
        this_month_15 = datetime.datetime(date.year,date.month,15,date.hour,date.minute,date.second)
        this_month_15_weekday = this_month_15.weekday() + 1
        if this_month_15_weekday == 6:
            cut_off_day = 17
        elif this_month_15_weekday == 7:
            cut_off_day = 16
        else:
            cut_off_day = 15
        if date.day>cut_off_day:
            for i in range(1,6):
                date_next = date+ dateutil.relativedelta.relativedelta(months=i)
                contract = getContractNum(date_next)
                contract = contract[2:6]
                pattern = 'PRODUCTNAME.*?铝.*?DELIVERYMONTH.*?' + contract + '.*?"SETTLEMENTPRICE.*?(\d+).*PRODUCTNAME.*?锌'
                res = crawler_futures(url, pattern)
                if not res:
                    return 0
                data = {
                    'contract': contract,
                    'fdate': dateString[:4] + '-' + dateString[4:6] + '-' + dateString[6:8],
                    'settlementprice': res[0],
                    'scrapy_time': datetime.datetime.now()
                }
                result_ls.append(data)
            print(result_ls)
            return result_ls
        else:
            for i in range(0,5):
                date_next = date+ dateutil.relativedelta.relativedelta(months=i)
                contract = getContractNum(date_next)
                contract = contract[2:6]
                pattern = 'PRODUCTNAME.*?铝.*?DELIVERYMONTH.*?' + contract + '.*?"SETTLEMENTPRICE.*?(\d+).*PRODUCTNAME.*?锌'
                res = crawler_futures(url, pattern)
                if not res:
                    return 0
                data = {
                    'contract': contract,
                    'fdate': dateString[:4] + '-' + dateString[4:6] + '-' + dateString[6:8],
                    'settlementprice': res[0],
                    'scrapy_time': datetime.datetime.now()
                }
                result_ls.append(data)
            print(result_ls)
            return result_ls
    else:
        return 0


def main():
    date = datetime.datetime(2020,4,23)
    print(date)
    for i in range(0,6):
        date_target = date+datetime.timedelta(days=i)
        print(date_target)
        res_one_day = crawler_one_day(date_target)
        if res_one_day != 0:
            pdResult = pd.DataFrame(res_one_day)
            pdResult.to_csv('上海期货-铝-新.csv', index=False, header=False, mode='a')


main()


# def job():
#     print("I'm working...")
#     main(0, 0)
#
# schedule.every(1).minutes.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
#



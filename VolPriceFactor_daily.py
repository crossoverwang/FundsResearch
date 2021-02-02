# -*- coding:utf-8 -*-
# from AlphaBacktest.src.util.dbhandler import DBHandler
from WindPy import *
import pandas as pd
import os
import numpy as np
import datetime as dt
import logging
__author__ = 'starcosmos'


class VolPriceFactor(object):
    def __init__(self, start_date, end_date, data_path):
        self.start_date = start_date
        self.end_date = end_date
        self.data_path = data_path
        self.EODPrice_data = None               # A股日行情数据
        self.L2Indicators_data = None           # A股Level2指标：主卖，主卖等
        self.stocks = None                      # 因子数据中的股票代码
        self.tradeDate = None                   # 因子数据中的日期（原始数据）
        self.newDate = None                     # 数据更新后的日日期

    def AShareEODPriceData_sql(self, connect_type, filename):
        '''
        将A股行情数据保存
        :param startDate:
        :param endDate:
        :param connect_type:
        :return:
        示例使用代码：
        ini_path = "D:/实习/多因子学习/AFactorCal.ini"
        data_path = 'D:/实习/多因子学习/AFactorCal/因子数据/'
        filename = 'AShareEODPrice.pickle'
        connect_type = 'inner'
        start_date = '20120101'
        end_date = '20171026'
        volpricefactor = VolPriceFactor(start_date, end_date, data_path)
        volpricefactor.write_base_data(start_date, end_date, connect_type, filename)
        '''
        ini_path = os.path.dirname(__file__) + "/AFactorCal.ini"
        dbHandler = DBHandler(ini_path, connect_type)
        db = dbHandler.get_db()
        cursor = db.cursor()

        fields = " WHERE TRADE_DT >= '" + self.start_date + "' AND TRADE_DT <= '" + self.end_date + "'"
        sqlstr = 'SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_CHANGE, \
                  S_DQ_PCTCHANGE, S_DQ_AMOUNT, S_DQ_VOLUME, S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJHIGH, S_DQ_ADJLOW, S_DQ_ADJCLOSE,\
                  S_DQ_ADJFACTOR, S_DQ_AVGPRICE, S_DQ_TRADESTATUS FROM C##WIND.ASHAREEODPRICES ' + fields
        cursor.execute(sqlstr)
        # db.commit()  # 提交修改
        data = cursor.fetchall()  # 返回tuple

        col_name = ['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_OPEN', 'S_DQ_HIGH', 'S_DQ_LOW', 'S_DQ_CLOSE', 'S_DQ_CHANGE', 'S_DQ_PCTCHANGE', \
                    'S_DQ_AMOUNT', 'S_DQ_VOLUME', 'S_DQ_ADJPRECLOSE', 'S_DQ_ADJOPEN', 'S_DQ_ADJHIGH', 'S_DQ_ADJLOW', 'S_DQ_ADJCLOSE', 'S_DQ_ADJFACTOR',\
                    'S_DQ_AVGPRICE', 'S_DQ_TRADESTATUS']
        data = pd.DataFrame(data, columns=col_name)
        path = self.data_path + filename
        data.to_pickle(path)
        # print(data)

    def InitRetdata(self, mergefile):
        temp_EODPrice_data = pd.read_pickle(self.data_path + 'AShareEODPrice_test.pickle')
        # 写入前复权(FORWARDS answer authority)
        temp_EODPrice_data['S_FWDS_ADJPRECLOSE'] = temp_EODPrice_data['S_DQ_ADJPRECLOSE'] / temp_EODPrice_data['S_DQ_ADJFACTOR']
        temp_EODPrice_data['S_FWDS_ADJOPEN'] = temp_EODPrice_data['S_DQ_ADJOPEN'] / temp_EODPrice_data['S_DQ_ADJFACTOR']
        temp_EODPrice_data['S_FWDS_ADJHIGH'] = temp_EODPrice_data['S_DQ_ADJHIGH'] / temp_EODPrice_data['S_DQ_ADJFACTOR']
        temp_EODPrice_data['S_FWDS_ADJLOW'] = temp_EODPrice_data['S_DQ_ADJLOW'] / temp_EODPrice_data['S_DQ_ADJFACTOR']
        temp_EODPrice_data['S_FWDS_ADJCLOSE'] = temp_EODPrice_data['S_DQ_ADJCLOSE'] / temp_EODPrice_data['S_DQ_ADJFACTOR']
        # df1 = temp_EODPrice_data[temp_EODPrice_data.S_INFO_WINDCODE == '600309.SH']
        # df2 = temp_L2Indicators_data[temp_L2Indicators_data.S_INFO_WINDCODE == '600309.SH']
        finaldata = temp_EODPrice_data[['S_INFO_WINDCODE','TRADE_DT','S_DQ_VOLUME','S_DQ_AMOUNT','S_FWDS_ADJPRECLOSE','S_FWDS_ADJOPEN','S_FWDS_ADJHIGH','S_FWDS_ADJLOW','S_FWDS_ADJCLOSE','S_DQ_AVGPRICE','S_DQ_TRADESTATUS']]
        # 将停牌的股票除了代码、日期和状态都设为nan
        reset_columns = [i for i in finaldata.columns if i not in ['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_TRADESTATUS']]
        finaldata.loc[finaldata['S_DQ_TRADESTATUS'] == '停牌', reset_columns] = np.nan
        # 防止乱序，按照日期升序列排列
        finaldata.sort_values(by=['S_INFO_WINDCODE', 'TRADE_DT'], ascending=True, inplace=True)
        self.stocks = np.unique(finaldata['S_INFO_WINDCODE'].values)  # unique返回升序后的唯一值
        self.tradeDate = np.unique(finaldata['TRADE_DT'].values)
        self.further = finaldata[['S_INFO_WINDCODE']].groupby(['S_INFO_WINDCODE']).size()
        self.S_DQ_VOLUME = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_AMOUNT = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJPRECLOSE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJOPEN = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJHIGH = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJLOW = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJCLOSE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_AVGPRICE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_TRADESTATUS = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        pre = 0
        lenthdate = len(self.tradeDate)
        for stock in self.stocks:
            stklen = self.further[stock]
            print(' stock is: ', stock)
            self.S_DQ_VOLUME.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_DQ_VOLUME'])
            self.S_DQ_AMOUNT.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_DQ_AMOUNT'])
            self.S_FWDS_ADJPRECLOSE.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_FWDS_ADJPRECLOSE'])
            self.S_FWDS_ADJOPEN.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_FWDS_ADJOPEN'])
            self.S_FWDS_ADJHIGH.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_FWDS_ADJHIGH'])
            self.S_FWDS_ADJLOW.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_FWDS_ADJLOW'])
            self.S_FWDS_ADJCLOSE.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_FWDS_ADJCLOSE'])
            self.S_DQ_AVGPRICE.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_DQ_AVGPRICE'])
            self.S_DQ_TRADESTATUS.iloc[lenthdate - stklen:][stock] = list(
                finaldata.iloc[pre:pre + stklen]['S_DQ_TRADESTATUS'])
            pre = pre + stklen
        usedata = {'S_DQ_VOLUME':self.S_DQ_VOLUME,
        'S_DQ_AMOUNT':self.S_DQ_AMOUNT,
        'S_FWDS_ADJPRECLOSE':self.S_FWDS_ADJPRECLOSE,
        'S_FWDS_ADJOPEN':self.S_FWDS_ADJOPEN,
        'S_FWDS_ADJHIGH':self.S_FWDS_ADJHIGH,
        'S_FWDS_ADJLOW':self.S_FWDS_ADJLOW,
        'S_FWDS_ADJCLOSE':self.S_FWDS_ADJCLOSE,
        'S_DQ_AVGPRICE':self.S_DQ_AVGPRICE,
        'S_DQ_TRADESTATUS':self.S_DQ_TRADESTATUS}
        usedata = pd.Panel(usedata)
        # to_pickle 存入数据
        usedata.to_pickle(self.data_path + mergefile)
        # 中国联通的数据做测试
        # finaldata=finaldata[finaldata['S_INFO_WINDCODE'] == '600050.SH']

    def Factor2Pickle(self, factor, factor_name):
        filename = self.data_path + factor_name + '.pickle'
        factor.to_pickle(filename)

    def InitRetdata_windApi(self, startDate, endDate):
        w.start()
        tradeDay =  w.tdays(startDate, endDate)
        tradeDay = tradeDay.Data[0]
        t = list(map(lambda x: x.date(), tradeDay))         # wind返回的是datetime类型，转换为date
        # 获取最后一天A股的股票wind
        startDate_str = startDate.strftime('%Y%m%d')
        endDate_str = endDate.strftime('%Y%m%d')
        paramStr = "date=" + endDate_str + ";sector=全部A股"
        # windCodes = w.wset("sectorconstituent", "date=2018-01-04;sector=全部A股")
        windCodes = w.wset("sectorconstituent", paramStr)
        windCodes = windCodes.Data[1]    # Data[0]:日期， Data[1]:股票代码， Data[2]：股票名称
        self.stocks = windCodes
        self.tradeDate = t
        self.S_DQ_VOLUME = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_AMOUNT = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJPRECLOSE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJOPEN = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJHIGH = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJLOW = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_FWDS_ADJCLOSE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_AVGPRICE = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        self.S_DQ_TRADESTATUS = pd.DataFrame(np.nan, index=self.tradeDate, columns=self.stocks)
        # wind行情数据获取
        fields = "PRE_CLOSE,OPEN,HIGH,LOW,CLOSE,VOLUME,AMT,VWAP,TRADE_STATUS"
        for windCode in windCodes:
            # 将api数据转化为pandas数据
            # wsd_data = w.wsd("603283.SH", fields, "2017-12-05", "2018-01-03", "PriceAdj=F")
            # print(windCode)
            wsd_data = w.wsd(windCode, fields, startDate_str, endDate_str, "PriceAdj=F")
            df = pd.DataFrame(wsd_data.Data, index=wsd_data.Fields, columns=wsd_data.Times)
            df = df.T
            df.loc[df['TRADE_STATUS'] != '交易'] = np.nan                 # 设置停牌数据为空值
            # 取新的日期和现有日期的并集
            # self.newDate = list(set(self.tradeDate).union(set(df.index)))
            self.S_DQ_VOLUME[windCode] = df['VOLUME']
            self.S_DQ_AMOUNT[windCode] = df['AMT']
            self.S_FWDS_ADJPRECLOSE[windCode] = df['PRE_CLOSE']
            self.S_FWDS_ADJOPEN[windCode] = df['OPEN']
            self.S_FWDS_ADJHIGH[windCode] = df['HIGH']
            self.S_FWDS_ADJLOW[windCode] = df['LOW']
            self.S_FWDS_ADJCLOSE[windCode] = df['CLOSE']
            self.S_DQ_AVGPRICE[windCode] = df['VWAP']
            self.S_DQ_TRADESTATUS[windCode] = df['TRADE_STATUS']
        filename = self.data_path + 'AShareEODPrice_' + endDate_str
        # self.S_FWDS_ADJCLOSE.to_pickle(filename)
        # print(self.S_FWDS_ADJCLOSE)
        usedata = {'S_DQ_VOLUME': self.S_DQ_VOLUME,
                   'S_DQ_AMOUNT': self.S_DQ_AMOUNT,
                   'S_FWDS_ADJPRECLOSE': self.S_FWDS_ADJPRECLOSE,
                   'S_FWDS_ADJOPEN': self.S_FWDS_ADJOPEN,
                   'S_FWDS_ADJHIGH': self.S_FWDS_ADJHIGH,
                   'S_FWDS_ADJLOW': self.S_FWDS_ADJLOW,
                   'S_FWDS_ADJCLOSE': self.S_FWDS_ADJCLOSE,
                   'S_DQ_AVGPRICE': self.S_DQ_AVGPRICE,
                   'S_DQ_TRADESTATUS': self.S_DQ_TRADESTATUS}
        usedata = pd.Panel(usedata)
        # to_pickle 存入数据
        usedata.to_pickle(filename)

    def getData_windApi(self, startDate, endDate):
        w.start()
        tradeDay = w.tdays(startDate, endDate)
        tradeDay = tradeDay.Data[0]
        t = list(map(lambda x: 10000*x.year + 100*x.month + x.day, tradeDay))  # wind返回的是datetime类型，转换为数字类型
        # 获取最后一天A股的股票wind
        startDate_str = startDate.strftime('%Y%m%d')
        endDate_str = endDate.strftime('%Y%m%d')
        paramStr = "date=" + endDate_str + ";sector=全部A股"
        # windCodes = w.wset("sectorconstituent", "date=2018-01-04;sector=全部A股")
        windCodes = w.wset("sectorconstituent", paramStr)
        windCodes = windCodes.Data[1]  # Data[0]:日期， Data[1]:股票代码， Data[2]：股票名称
        self.stocks = windCodes
        self.tradeDate = t

        # wind行情数据获取
        colName_api = ['S_INFO_WINDCODE', 'TRADE_DT', 'PRE_CLOSE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'VWAP', 'TRADE_STATUS']
        fields = "PRE_CLOSE,OPEN,HIGH,LOW,CLOSE,VOLUME,AMT,VWAP,TRADE_STATUS"
        finaldata = pd.DataFrame(columns=colName_api)
        for windCode in windCodes:
            print(windCode)
            wsd_data = w.wsd(windCode, fields, startDate_str, endDate_str, "PriceAdj=F")
            df = pd.DataFrame(wsd_data.Data, index=wsd_data.Fields, columns=wsd_data.Times)
            df = df.T
            try:
                df.insert(0, 'TRADE_DT', self.tradeDate)
                df.insert(0, 'S_INFO_WINDCODE', windCode)
            except Exception as e:
                logging.exception(e)
                continue
            df.loc[df['TRADE_STATUS'] != '交易'] = np.nan  # 设置停牌数据为空值
            finaldata = pd.concat([finaldata, df], ignore_index=True)

        colName_use = ['S_INFO_WINDCODE', 'TRADE_DT', 'S_FWDS_ADJPRECLOSE', 'S_FWDS_ADJOPEN', 'S_FWDS_ADJHIGH',
                       'S_FWDS_ADJLOW', 'S_FWDS_ADJCLOSE', 'S_DQ_VOLUME', 'S_DQ_AMOUNT', 'S_DQ_AVGPRICE', 'S_DQ_TRADESTATUS']
        finaldata.columns = colName_use
        filename = self.data_path + 'AShareEODPrice_' + endDate_str + '.pickle'
        finaldata.to_pickle(filename)


if __name__ == '__main__':
    start_time = dt.datetime.now()
    data_path = os.path.dirname(os.path.dirname(__file__)) + '/results/'
    connect_type = 'inner'
    start_date = dt.date(2017, 11, 29)  # dt.date(2018, 1, 5)
    end_date = dt.date(2018, 1, 8)
    volpricefactor = VolPriceFactor(start_date, end_date, data_path)
    f3 = 'AShareMerge_test.pickle'
    volpricefactor.getData_windApi(start_date, end_date)
    end_time = dt.datetime.now()
    print((end_time - start_time).seconds)

    '''	
if __name__ == '__main__':
    data_path = os.path.dirname(os.path.dirname(__file__)) + '/results/'
    connect_type = 'inner'
    start_date = '20120101'
    end_date = '20171128'
    volpricefactor = VolPriceFactor(start_date, end_date, data_path)
    f1 = 'AShareEODPrice_test.pickle'
    f2 = 'AShareL2Indicators_test.pickle'
    volpricefactor.AShareEODPriceData(connect_type, f1)
    volpricefactor.AShareL2IndicatorsData(connect_type, f2)
    f3 = 'AShareMerge_test.pickle'
    volpricefactor.InitRetdata(f3)
    '''
'''
startDate = dt.date(2017,12,28)
endDate = dt.date(2018,1,7)
startDate_str = startDate.strftime('%Y%m%d')
endDate_str = endDate.strftime('%Y%m%d')
tradeDay = w.tdays(startDate, endDate)
tradeDay = tradeDay.Data[0]
t = list(map(lambda x: 10000*x.year + 100*x.month + x.day, tradeDay))

colName_api = ['S_INFO_WINDCODE', 'TRADE_DT', 'PRE_CLOSE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'VWAP', 'TRADE_STATUS']
finaldata = pd.DataFrame(columns=colName_api)
wsd_data = w.wsd("603283.SH", fields, "2017-12-28", "2018-01-07", "PriceAdj=F")
df = pd.DataFrame(wsd_data.Data, index=wsd_data.Fields, columns=wsd_data.Times)
df = df.T
df.insert(0, 'TRADE_DT', t)
df.insert(0, 'S_INFO_WINDCODE', "603283.SH")
finaldata = pd.concat([finaldata, df], ignore_index=True)

wsd_data = w.wsd("600297.SH", fields, "2017-12-28", "2018-01-07", "PriceAdj=F")
df = pd.DataFrame(wsd_data.Data, index=wsd_data.Fields, columns=wsd_data.Times)
df = df.T
df.insert(0, 'TRADE_DT', t)
df.insert(0, 'S_INFO_WINDCODE', "600297.SH")
finaldata = pd.concat([finaldata, df], ignore_index=True)
'''
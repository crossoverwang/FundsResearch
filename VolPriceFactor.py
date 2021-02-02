# -*- coding:utf-8 -*-
from AlphaBacktest.src.util.dbhandler import DBHandler
import pandas as pd
import os
import numpy as np
from time import strftime
__author__ = 'starcosmos'


class VolPriceFactor(object):
    def __init__(self, data_path):
        self.data_path = data_path
        self.EODPrice_data = None               # A股日行情数据
        self.L2Indicators_data = None           # A股Level2指标：主卖，主卖等
        self.stocks = None                      # 因子数据中的股票代码
        self.tradeDate = None                   # 因子数据中的日期

    def eachFile(self, filepath):
        file = []
        filepath = filepath + 'eodpricetest/'
        pathDir = os.listdir(filepath)
        for allDir in pathDir:
            child = os.path.join('%s%s' % (filepath, allDir))
            childDir = os.listdir(child + '/')
            mergechild = [child + '/' + i for i in childDir]
            file.extend(mergechild)
        return file

    def merger(self, filepath):
        files = self.eachFile(filepath)
        label = pd.DataFrame()
        for child in files:
            dailyEODPrice_data = pd.read_pickle(child)
            reset_columns = [i for i in dailyEODPrice_data.columns if i not in ['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_TRADESTATUS']]
            dailyEODPrice_data.loc[dailyEODPrice_data['S_DQ_TRADESTATUS'] != '交易', reset_columns] = np.nan
            dailyEODPrice_data['TRADE_DT'] = pd.Series(dailyEODPrice_data['TRADE_DT'], dtype='str')
            label = pd.concat([label, dailyEODPrice_data], axis=0)
        return label

    def InitRetdata(self, mergefile):
        '''
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
        '''
        finaldata = self.merger(self.data_path)                                    # 获取日线数据，每日增量数据，进行校准
        # finaldata = pd.concat([finaldata, dailyEODPrice_data], axis=0)                      # 和全量数据数据进行merge
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


if __name__ == '__main__':
    data_path = os.path.dirname(os.path.dirname(__file__)) + '/results/'
    volpricefactor = VolPriceFactor(data_path)
    #today = strftime("%Y%m%d")
    today='20190412'
    f3 = '/mergetest/AShareMerge_test' + today +  '.pickle'
    volpricefactor.InitRetdata(f3)

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 16:31:10 2018

@author: admin
"""
import os 
import time
import shutil
import calendar
import numpy as np
import pandas as pd
import pandas.tseries.offsets as toffsets
from WindPy import w
from functools import wraps
from factor_calculate import FactorProcess, WindQueryFailError

START_YEAR = 2006

FPATH = os.path.dirname(os.path.dirname(__file__))
MPATH = os.path.join(FPATH, "monthly_data")
QPATH = os.path.join(FPATH, "quarterly_data")
DPATH = os.path.join(FPATH, "daily_data")

def backup_decorator(dirname=None):
    def inner(func):
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            self.make_backup(dirname)
            try:
                func(self, *args, **kwargs)
                print(f"\nUpdate {dirname} complete.")
            except Exception as e:
                print("Error happened during {} update, msg:{}".format(dirname, e))
                self.restore_backup(dirname)
                raise
        return wrapped
    return inner

class UpdateOriginData(FactorProcess): 
    backup_path = os.path.join(os.path.dirname(FPATH), 'wind_factor_backup')
    
    def __init__(self, *args, **kwargs):
        kwargs['update_only'] = True
        super().__init__(*args, **kwargs)

    def get_listday_matrix(self):
        all_stocks_info = self.meta
        trade_days = self.close.columns.tolist()
        
        def if_listed(series):
            nonlocal all_stocks_info
            code = series.name
            ipo_date = all_stocks_info.at[code, 'ipo_date']
            delist_date = all_stocks_info.at[code, 'delist_date']
            daterange = series.index
            if delist_date is pd.NaT:
                res = np.where(daterange >= ipo_date, 1, 0)
            else:
                res = np.where(daterange < ipo_date, 0, np.where(daterange <= delist_date, 1, 0))
            return pd.Series(res, index=series.index) 
            
        listday_dat = pd.DataFrame(index=all_stocks_info.index, columns=trade_days)
        listday_dat = listday_dat.apply(if_listed, axis=1)
        self.close_file(listday_dat, 'listday_matrix')

    def show_message(self, st):
        *_, size, atime, mtime, ctime = st
#        print("- size:", size, "bytes")
        print('\n', 'Current backup date msg:')
        print("- created:", time.ctime(ctime))
        print("- last accessed:", time.ctime(atime))
        print("- last modified:", time.ctime(mtime))
        
    def make_backup(self, dirname, backup_path=None):
        if backup_path is None:
            backup_path = self.backup_path
        if not os.path.exists(backup_path):
            os.mkdir(backup_path)
        
        SOURCE = os.path.join(self.root, dirname)
        DESTINATION = os.path.join(backup_path, dirname)
        
        if os.path.exists(DESTINATION):
            self.show_message(os.stat(DESTINATION))
            ans = input(f"DELETE previous {dirname} data under backup dir, proceed?[y/n]")
            while ans != "y":
                ans = input("""Must DELETE previous backup data to release 
                            enough space for new backup, proceed?[y/n]""")
            shutil.rmtree(DESTINATION)

        dst = shutil.copytree(SOURCE, DESTINATION)
        print(f"Create new backup from \'{SOURCE}\' to \'{dst}\' successfully.")
    
    def restore_backup(self, dirname, backup_path=None):
        if backup_path is None:
            backup_path = self.backup_path
            
        SOURCE = os.path.join(backup_path, dirname)
        DESTINATION = os.path.join(self.root, dirname)
        
        if not os.path.exists(SOURCE):
            raise ValueError("Must make backup before restoring!")
            
        print("Backup version message: ")
        self.show_message(os.stat(SOURCE))
        ans = input(f"""Current backup of \'{dirname}\' will cover original data. 
                    Proceed?[y/n]""")
        if ans == 'y':
            if os.path.exists(DESTINATION):
                shutil.rmtree(DESTINATION)
            dst = shutil.copytree(SOURCE, DESTINATION)
            print(f"Restore backup \'{SOURCE}\' to \'{dst}\' successfully.")
        
    def update_all(self):
        w.start()
        date = toffsets.datetime.now().date()
        self.make_backup(dirname='src')
        try:
            self.update_meta_data(date)
            self.update_month_map_data(date)
        except Exception as e:
            self.restore_backup(dirname='src')
            print("""During updating meta data an error happend, msg: {}.
                  Please retry with other Wind account if the error's happening 
                  is due to exceeding the usage limit.""".format(e))
            return 
        self.update_daily_data()
        self.update_monthly_data()
        print("Update All Data Successfully!")
        w.close()
        
    def _get_trade_day(self, caldate):
        res = w.tdaysoffset(0, caldate, )
        if res.ErrorCode != 0:
            raise WindQueryFailError("Get date data failed, errorcode={}.".format(res.ErrorCode))
        return res.Data[0][0]
    
    def _get_month_end(self, date):
        _, days = calendar.monthrange(date.year, date.month)
        if date.day == days:
            return date
        else:
            return date + toffsets.MonthEnd(n=1)
            
    def update_month_map_data(self, cur_date=None):
        if cur_date is None:
            cur_date = toffsets.datetime.now().date()
        lst_date = self.month_map.index[-1]
        
        if cur_date.year == lst_date.year:
            update = (cur_date.month - lst_date.month) >= 2
        elif (cur_date.year - lst_date.year) > 0 :
            update = (12 * (cur_date.year - lst_date.year - 1 ) + \
                     (cur_date.month - 1) + (12 - lst_date.month)) >= 1
        else:
            update = False
            
        if update:
            lst_date += toffsets.timedelta(weeks=2)
            new_tdays = self._get_trade_days(lst_date, cur_date, "M")
            if len(new_tdays) > 1:
                new_tdays = new_tdays[:-1]
            new_caldays = [self._get_month_end(tdate) for tdate in new_tdays]
            new_dates = pd.Series(new_caldays, index=new_tdays)
        else:
            print("Month_map data need not to be updated.")
            return
            
        month_map = self.month_map.append(new_dates).reset_index()
        month_map.columns = ["trade_date", "calendar_date"]
        month_map.set_index(['trade_date'], inplace=True)
        self.close_file(month_map, 'month_map')
        self.create_month_tdays_begin_end(month_map.index[-1])
        self.update_monthgroup()
        print("Update month_map complete.")
    
    def update_monthgroup(self):
        mg = self.month_group
        tdays_be_m = self.trade_days_begin_end_of_month
        lst_me = mg.index[-1]
        new_mes = tdays_be_m.loc[lst_me:].index.tolist()[1:]
        for me in new_mes:
            new_me_dat = mg.loc[f'{me.year-1}-{me.month}'] + 1
            new_me_dat.index = [me]
            mg = pd.concat([mg, new_me_dat])
        self.close_file(mg, 'month_group')
    
    def create_month_tdays_begin_end(self, latest_month_end_tradeday=None):
        tdays = self.close.columns.tolist()
        months_start = tdays[0:1] + list(after_d for before_d, after_d in zip(tdays[:-1], tdays[1:]) 
                                         if before_d.month != after_d.month)
        months_end = list(before_d for before_d, after_d in zip(tdays[:-1], tdays[1:]) 
                          if before_d.month != after_d.month) + tdays[-1:]
        if latest_month_end_tradeday is None:
            latest_month_end_tradeday = self.month_map.index[-1]
        if months_end[-1] > latest_month_end_tradeday:
            months_start, months_end = months_start[:-1], months_end[:-1]
        trade_days_be_month = pd.DataFrame(months_end, index=months_start, 
                                           columns=['month_end'])
        trade_days_be_month.index.name = 'month_start'
        self.close_file(trade_days_be_month, 'trade_days_begin_end_of_month')

    def update_meta_data(self, date=None):
        if date is None:
            date = toffsets.datetime.now().date()
        lsttdate = self._get_date(date, datelist=self.tradedays)
        date = str(lsttdate)[:10]
        ori_meta = getattr(self, "meta",).copy()
        del ori_meta['delist_date']

        res = w.wset("sectorconstituent",f"date={date};sectorid=a001010100000000;field=wind_code,sec_name")
        if res.ErrorCode != 0:
            raise WindQueryFailError("Updating meta data failed, errorcode={}".format(res.ErrorCode))
        res = pd.DataFrame(res.Data, index=res.Fields).T.set_index(['wind_code'])
        
        codes_to_update = res.index.difference(ori_meta.index)
        if codes_to_update.empty:
            print("Meta data don't need to update.")
            return 
        ipo_dates_append = self._get_data_from_windpy(codes_to_update, "ipo_date", None, "ipodate")
        new_meta = self.concat_df(res.loc[codes_to_update, ], ipo_dates_append)  
        new_meta = pd.concat([new_meta, ori_meta]).sort_index()
        
        new_meta['delist_date'] = self._get_data_from_windpy(new_meta.index, 
                            "delist_date", f"{date};{date};", 'wsd-delist_date')
        self.close_file(new_meta, 'meta')
        print("Update meta data complete.")
    
    def _update_new_data(self, ori_data, tdays, stockslist, qname, freq):
        qname = "_".join(qname.split('_')[:-1]) if qname.endswith('_d') else qname
        wsscond = self.ind_wsscond[qname]
        wsdcond = self.ind_wsdcond[qname]
        t_to_c_dict = {tday:cday for tday, cday in self.month_map.to_dict().items()}

        if qname in ('close', 'pct_chg'):
            stockslist.extend(['000001.SH', '000300.SH', '000905.SH'])
        if ori_data is None:
            ori_data = pd.DataFrame(index=stockslist)
            
        new_cols = []
        for date in tdays[::-1]:
            if date in ori_data.columns or ((date in self.month_map.index) and \
                                (self.month_map[date] in ori_data.columns)):
                continue
            qdate = "".join(str(date)[:10].split("-"))
            try:
                dat = self._get_data_from_windpy(stockslist, qname, wsscond.format(date=qdate), qname)
            except WindQueryFailError:
                qdate = str(date)[:10]
                try:
                    dat = self._get_data_from_windpy(stockslist, qname, wsdcond.format(date=qdate), "wsd_"+qname)
                except WindQueryFailError:
                    print("Update {} data interrupted.".format(qname))
                    break
            dat.columns = [date] 
            new_cols.extend(dat.columns)
            ori_data = self.concat_df(ori_data, dat)
        try:
            ori_data.columns = pd.to_datetime(ori_data.columns)
        except Exception:
            print(qname)
            raise
        ori_data = ori_data[ori_data.columns.sort_values()]
        if freq != 'd':
            ori_data.columns = [t_to_c_dict.get(d, d) for d in ori_data.columns]
        return new_cols, ori_data

    def update_ori_data(self, fname, freq, stockslist=None, new_date=None,
                        start_date=None, end_date=None, include_today=False):
        try:
#            self = z; fname = qname; freq='M'
            
            ori_data = getattr(self, fname, None)
            ori_periods = ori_data.columns.sort_values()
            ori_sdate, ori_edate = ori_periods[0], ori_periods[-1]
        except:
            raise Exception(f'{fname} not found or specific error encountered while parsing data structure.')
            
        if start_date and end_date:
            start_date, end_date = pd.to_datetime((start_date, end_date))
            tdays = self._get_update_periods(start_date, end_date, ori_sdate, ori_edate, freq)
            if stockslist is None:
                stks_delisted = self.meta['delist_date'].apply(lambda x: 1 if x < start_date else 0)
                stks_listed = self.meta['ipo_date'].apply(lambda x: 1 if x > end_date else 0)
                stockslist = self.meta[(stks_delisted == 0) & (stks_listed == 0)].index.tolist()
            update_past = False
        else:
            lst_date = ori_edate 
            curdate = toffsets.datetime.now().date() 
            tdays = self._get_trade_days(lst_date, curdate, freq=freq)
            if len(tdays) < 1:
                return None, None

            if new_date is None:
                new_date = curdate
            else:
                new_date = pd.to_datetime(new_date)
            update_past = (freq == 'q') or \
                          (freq == 'M' and (new_date.month - lst_date.month) <= 1) or \
                          (freq == 'd')
            
            if update_past:
                sec_lst_date = ori_periods[-2]
                del ori_data[lst_date]
                del ori_data[sec_lst_date]
                
            lst_date = ori_data.columns[-1]
            tdays = self._get_trade_days(lst_date, new_date, freq=freq)
            
            if stockslist is None:
                val_stks = self.meta['delist_date'].apply(lambda x: 1 if x < lst_date else 0)
                stockslist = val_stks[val_stks == 0].index.tolist()
            
            if freq == "M":
                tdays = [d for d in tdays if d in self.month_map.index and d != lst_date]
            elif freq == 'q':
                tdays = [self.month_map[d] for d in tdays if (d in self.month_map.index) \
                         and (d.month in (3, 6, 9, 12))]
            else:
                if include_today and new_date == curdate:
                    tdays = tdays[1:]
                else:
                    tdays = tdays[1:-1]
            
        qname = 'pct_chg' if fname.startswith("pct_chg") else fname.split(".")[0]
        if tdays:
            return self._update_new_data(ori_data, tdays, stockslist, qname, freq)
        else:
            return None, None
    
    def _get_update_periods(self, startday, endday, ori_sdate, ori_edate, freq):
        append_days = []
        if ori_sdate and ori_edate:
            startday, endday, ori_sdate, ori_edate = pd.to_datetime((startday, 
                                                     endday, ori_sdate, ori_edate))
            if startday < ori_sdate:
                if endday <= ori_edate:
                    endday = ori_sdate - toffsets.timedelta(days=1)
                    append_days.extend(self._get_trade_days(startday, endday, freq))
                else:
                    endday1 = ori_sdate - toffsets.timedelta(days=1)
                    startday1 = ori_edate + toffsets.timedelta(days=1)
                    append_days1 = self._get_trade_days(startday, endday1, freq)
                    append_days2 = self._get_trade_days(startday1, endday, freq)
                    append_days.extend(append_days1 + append_days2)
            elif ori_sdate <= startday <= ori_edate:
                if endday > ori_edate:
                    startday = ori_edate + toffsets.timedelta(days=1)
                    append_days.extend(self._get_trade_days(startday, endday, freq))
            else:
                startday = ori_edate + toffsets.timedelta(days=1)
                append_days.extend(self._get_trade_days(startday, endday, freq))
        else:
            append_days.extend(self._get_trade_days(startday, endday, freq))
        
        if freq in ('q','M'):
            append_days = [self.month_map[d] for d in append_days if d in self.month_map.index]
            try:
                idx1 = append_days.index(ori_sdate)
            except ValueError:
                try:
                    idx2 = append_days.index(ori_edate)
                except ValueError:
                    pass
                else:
                    append_days = append_days[idx2+1:]
            else:
                append_days = append_days[:idx1]
                
        return append_days
    
    @staticmethod
    def get_offset_date(series, date, n):
        dates = series.index.tolist()
        idx = dates.index(date)
        return series.iloc[idx - n + 1]
    
    def _update_pct_chg_nm(self, hfq_close=None, start_year='2008', end_year='2019'):
        benchmarks = ['000001.SH', '000300.SH', '000905.SH']
        tdays_be_month = self.trade_days_begin_end_of_month
        months_end = tdays_be_month.index

        #***fix hfq_close
        if hfq_close is None:
            hfq_close = self.hfq_close 
        hfq_close = hfq_close.T.fillna(method='ffill').T
        hfq_close = pd.concat([hfq_close, self.close.loc[benchmarks, hfq_close.columns]])
        self.close_file(hfq_close, 'hfq_close')
        #***pct_chg_M
        pct_chg_M = pd.DataFrame()
        for m_end_date in months_end:
            m_start_date = tdays_be_month.loc[m_end_date].values[0]
            pct_chg_M[self.month_map.loc[m_end_date]] = hfq_close[m_end_date] / hfq_close[m_start_date] - 1
        self.close_file(pct_chg_M, 'pct_chg_M')
        
        #pct_chg_Nm
        for period in (1,3,6,12):
            pct_chg_Nm = pd.DataFrame()
            if period != 1: 
                for m_end_date in months_end[::-1]:
                    try:
                        start_date_before_n_period = tdays_be_month.loc[self._get_date(m_end_date, -period+1, months_end)].values[0]
                        s = hfq_close[m_end_date] / hfq_close[start_date_before_n_period] - 1
                        pct_chg_Nm[self.month_map[m_end_date]] = s
                    except KeyError:
                        print(m_end_date)
                        break
            else:
                pct_chg_Nm = getattr(self, f'pct_chg_M', None)
                
            self.close_file(pct_chg_Nm, f"pctchg_{period}M")
            print(f'pct_chg_{period}M updated.')
    
    @backup_decorator(dirname='daily_data')
    def update_daily_data(self, stockslist=None, date=None, start_date=None, end_date=None,
                          include_today=False):
        inds_to_update = ('pct_chg', 'close', 'adjfactor', 'maxupordown', 
                          'trade_status', 'turn', 'amt', 'dealnum', 'mkt_cap_ard', 
                          'mkt_cap_float_d')
        
        weekly_inds_to_update = ('close', 'adjfactor', 'maxupordown', 'pct_chg', 
                          'trade_status', 'turn', 'dividendyield2_d','mkt_cap_float_d',
                          'pb_lf_d', 'pcf_ncf_ttm_d', 'pcf_ocf_ttm_d', 'pe_ttm_d', 
                          'profit_ttm_d', 'ps_ttm_d', 'sec_name1_d', 'val_pe_deducted_ttm_d',
                          'industry_citic_d', 'industry_citic_level2_d')
        
        if self.updatefreq == 'w':
            inds_to_update = weekly_inds_to_update

        for qname in inds_to_update:
            new_cols, new_data = self.update_ori_data(qname, 'd', stockslist, date,
                                                      start_date, end_date, include_today)
            if new_cols:
                new_date = sorted(new_cols)[-1]
                if qname == 'trade_status':
                    new_data.loc[:, new_cols] = new_data.loc[:, new_cols].\
                                            applymap(lambda x: 0 if x != '交易' else 1)
                elif qname == 'pct_chg' or qname == 'turn':
                    new_data.loc[:, new_cols] = new_data.loc[:, new_cols] / 100
                self.close_file(new_data, qname)
                print("\"{}\" data updated to date {}.".format(qname, str(new_date)[:10]))
            else:
                print(f"\"{qname}\"'s data don't need to be updated.")
        
        close, adjfactor = self._align_element(self.close, self.adjfactor)
        hfq_close = close * adjfactor
        self.close_file(hfq_close, 'hfq_close')
        print("\'hfq_close\' updated.")
        
        self.get_listday_matrix()
        print("'listday matrix' updated.")
        
        if self.updatefreq == 'M':
            self._update_pct_chg_nm(hfq_close)
            
            amt, dealnum = self._align_element(self.amt, self.dealnum)
            amt_per_deal = amt / dealnum
            self.close_file(amt_per_deal, 'amt_per_deal')
            print("'amt_per_deal' updated")
            
            self._align_month_end_to_calendar()

        if self.updatefreq == 'w':
            datelist = hfq_close.columns.tolist()
            
            lastThursday =  toffsets.datetime.now()
            daydelta = toffsets.DateOffset(n=1)
            while lastThursday.weekday() != calendar.THURSDAY:
                lastThursday -= daydelta

            profit_ttm_G_d = self.profit_ttm_G_d
            update_dates = hfq_close.loc[:, profit_ttm_G_d.columns[-1]:lastThursday].columns[1:]
            yoy = pd.DataFrame()
            for date in update_dates:
                lstdate = toffsets.datetime(date.year-1, date.month, date.day)
                lstdate = self._get_date(lstdate, 0, datelist)
                yoy[date] = self.profit_ttm_d[date] / self.profit_ttm_d[lstdate] - 1
            profit_ttm_G_d = pd.concat([profit_ttm_G_d, yoy], axis=1)
            profit_ttm_G_d = profit_ttm_G_d[profit_ttm_G_d.columns.sort_values()]
            self.close_file(profit_ttm_G_d, 'profit_ttm_G_d')
            print("'profit_ttm_G_d' updated.")

            for offset in [1,3,6,12]:   
                pctchg_d = getattr(self, f'pctchg_{offset}M_d', )
                res = pd.DataFrame()

                update_dates = hfq_close.loc[:, pctchg_d.columns[-1]:lastThursday].columns[1:]
                for date in update_dates:    
                    if offset == 12:
                        lstyear = date.year - 1
                        lstmonth = date.month
                    else:
                        if date.month - offset > 0:
                            lstyear = date.year
                            lstmonth = date.month - offset
                        else:
                            lstyear = date.year - 1
                            lstmonth = date.month - offset + 12
                        lstday = min(date.day, calendar.monthrange(lstyear, lstmonth)[1])
                    lstdate = toffsets.datetime(lstyear, lstmonth, lstday)
                    lstdate = self._get_date(lstdate, 0, datelist)
                    res[date] = hfq_close[date] / hfq_close[lstdate] - 1
                
                pctchg_d = pd.concat([pctchg_d, res], axis=1)
                pctchg_d = pctchg_d[pctchg_d.columns.sort_values()]
                self.close_file(pctchg_d, f'pctchg_{offset}M_d')
                print(f"'pctchg_{offset}M_d' updated.")
        
    @backup_decorator(dirname='monthly_data')
    def update_monthly_data(self, stockslist=None, date=None, start_date=None, end_date=None):
        inds_to_update = ('sec_name1', 'industry_citic', 'industry_citic_level2', 
                'mkt_cap_float', 'pe_ttm', 'val_pe_deducted_ttm', 'ps_ttm', 
                'pb_lf', 'profit_ttm', 'pcf_ncf_ttm', 'pcf_ocf_ttm', 
                'dividendyield2', 'or_ttm','deductedprofit_ttm', 'ocfps_ttm', 
                'eps_ttm', 'holder_num', 'holder_avgpct', 'pct_chg_M')

#        self.update_quarterly_data()
#        self.qdata_to_mdata((start_date and end_date))
#        
        curdate = toffsets.datetime.now().date() if date is None else date
        ndate = curdate - toffsets.MonthEnd(n=1)
        for qname in inds_to_update:
            new_cols, new_data = self.update_ori_data(qname, 'M', stockslist, date,
                                                      start_date, end_date)
            if new_cols:
                if len(new_cols) == 1:
                    fill_cols = new_data.columns[-2:]        
                    new_data.loc[:, fill_cols] = new_data.loc[:, fill_cols].\
                                                 fillna(axis=1, method='ffill')
                self.close_file(new_data, qname)
                print("\"{}\" data updated to date {}.".format(qname, str(ndate)[:10]))
            else:
                print(f"\"{qname}\"'s data don't need to be updated.")
        
        #profit_ttm_G
        profit_ttm_G = self.profit_ttm.T / self.profit_ttm.T.shift(12) - 1
        profit_ttm_G = profit_ttm_G.T.dropna(how='all', axis=1)
        self.close_file(profit_ttm_G, "profit_ttm_G")
        print("'profit_ttm_G' updated.")
        
        #holder_avgpctchg
        holder_avgpct_cal = 1000 / self.holder_num
        holder_avgpct_cal, holder_avgpct_get = self._align_element(holder_avgpct_cal, self.holder_avgpct)
        orival, fillval = holder_avgpct_get.values, holder_avgpct_cal.values
 
        newval = np.where(np.isnan(orival), fillval, orival)
        holder_avgpct_fill = pd.DataFrame(newval, index=holder_avgpct_get.index, 
                                          columns=holder_avgpct_get.columns) 
        self.close_file(holder_avgpct_fill, "holder_avgpct_fill")

        h_fill = holder_avgpct_fill.T
        holder_avgpctchg = h_fill / h_fill.shift(12) - 1
        holder_avgpctchg = holder_avgpctchg.T.dropna(how='all', axis=1)
        self.close_file(holder_avgpctchg, "holder_avgpctchg")
        print("'holder_avgpct' updated.")
                
    def _align_month_end_to_calendar(self):
        for f in os.listdir(MPATH)[:]:   
            if 'mrq' in f or 'pctchg' in f:
                continue
            fname = f.split('.')[0]
            if fname in ('industry2',):
                continue
            
            tmp = getattr(self, fname, None)
            lst4dates = [pd.to_datetime(d) for d in self.month_map.values[-4:]]
            
            dat_lst_date, dat_4thlst_date = str(tmp.columns[-1])[:7], str(tmp.columns[-4])[:7]
            meta_lst_date, meta_4thlst_date = str(self.month_map.values[-1])[:7], str(self.month_map.values[-4])[:7]
            assert dat_lst_date == meta_lst_date, fname
            assert dat_4thlst_date == meta_4thlst_date, fname
            
            tmp.columns = sorted(tmp.columns[:-4].tolist() + list(lst4dates))            
            self.close_file(tmp, fname)

    @backup_decorator(dirname='quarterly_data')
    def update_quarterly_data(self, stockslist=None, date=None, 
                              start_date=None, end_date=None):
        inds_to_update = ('assetstoequity', 'cashtocurrentdebt', 'current', 'longdebttodebt',
            'grossprofitmargin_ttm2', 'longdebttoequity', 'qfa_deductedprofit', 'orps', 'eps_diluted2',
            'qfa_grossprofitmargin', 'qfa_netprofitmargin', 'qfa_net_cash_flows_oper_act',
            'qfa_net_profit_is', 'qfa_oper_rev', 'qfa_roa', 'qfa_roe', 'qfa_yoyocf',
            'qfa_yoyprofit', 'qfa_yoysales', 'roa2_ttm2', 'roe_ttm2', 'stm_issuingdate',
            'turnover_ttm', 'tot_equity', 'tot_liab', 'tot_assets', 'other_equity_instruments_PRE')

        curdate = toffsets.datetime.now().date() if date is None else date
        offset = curdate.month % 3 if (curdate.month % 3 != 0) else (curdate.month % 3 + 3)
        ndate = curdate - toffsets.MonthEnd(n=offset)
        
        for qname in inds_to_update:
            new_cols, new_data = self.update_ori_data(qname, 'q', stockslist, 
                                                      date, start_date, end_date)
            if new_cols:
                self.close_file(new_data, qname)
                print("\"{}\" data updated to date {}.".format(qname, str(ndate)[:10]))
            else:
                print(f"\"{qname}\"'s data don't need to be updated.")
    
    def qdata_to_mdata(self, update_past=False):
        self.update_real_rptdate('M')
        inds_to_transfer = [f.split(".")[0] for f in os.listdir(QPATH) \
                            if not f.startswith("stm") and not 'mrq' in f and '~' not in f] 
        cur_caldates = self.month_map.tolist()
        val_date = self.applied_rpt_date_M
        
        for fname in inds_to_transfer:       
            ori_q_dat = getattr(self, fname,)
            if fname in ('longdebttodebt','tot_equity', 'tot_liab', 
                         'tot_assets', 'other_equity_instruments_PRE'):
                dat_lyr = self._to_lyr(ori_q_dat)
                self.close_file(dat_lyr, fname+'_lyr')
                print("{} updated.".format(fname+'_lyr'))
                continue
            try:
                ori_m_dat = getattr(self, fname+'_m', None)
            except Exception:
               ori_m_dat = pd.DataFrame(index=ori_q_dat.index)
               for date in cur_caldates[::-1]:
                    val = val_date[[date]].dropna()
                    try:
                        tmp = [ori_q_dat.at[code, date] for code, date in zip(val.index, val.values.flatten())]
                    except Exception:
                        print(f'{date} missed in creating {fname}\'s monthly data.')
                        break
                    ori_m_dat[date] = pd.Series(tmp, index=val.index)
            else:
                ori_m_dat = ori_m_dat[ori_m_dat.columns[:-2]]
                try:
                    idx = cur_caldates.index(ori_m_dat.columns[-1])
                except ValueError:
                    print("Please update monthmap first!")
                    break
                
                if update_past:
                    all_dates = set(cur_caldates[:]) if fname == 'qfa_roe' else set(cur_caldates[12:])
                    past_dates = all_dates - set(ori_m_dat.columns)
                    chg_dates = sorted(past_dates)
                else:
                    chg_dates = cur_caldates[idx:]
                
#                chg_dates = cur_caldates
#                ori_m_dat = pd.DataFrame()
                for date in chg_dates[::-1]:
                    val = val_date[[date]].dropna()
                    try:
                        tmp = [ori_q_dat.at[code, date] for code, date in zip(val.index, val.values.flatten())]
                    except:
                        print(fname)
                        raise
                    ori_m_dat[date] = pd.Series(tmp, index=val.index)
                
            ori_m_dat = ori_m_dat[ori_m_dat.columns.sort_values()]
            self.close_file(ori_m_dat, fname+'_m')
            print("{} updated.".format(fname+'_m'))
        
        qfa_roe_G_m = self.qfa_roe_m.T / self.qfa_roe_m.T.shift(12) - 1
        qfa_roe_G_m = qfa_roe_G_m.T.dropna(how='all', axis=1)
        self.close_file(qfa_roe_G_m, "qfa_roe_G_m")
        print("\nUpdate q_to_m data complete.\n")
    
    def _to_lyr(self, datdf):
        start_year = START_YEAR
        if datdf.index.dtype == 'O':
            datdf = datdf.T
        curstart_year = start_year - 2
        datdf = datdf.loc[str(curstart_year):,]
        
        annual_rpt_date = [d for d in datdf.index if d.month == 12 and d.day == 31]
        annual_rpt_data = datdf.loc[annual_rpt_date,:]
        syear, eyear = annual_rpt_date[0].year, annual_rpt_date[-1].year+1
        month_group = self.month_group.loc[str(syear):str(eyear), ['Q4-1', 'Q4-2']]
        year_group = month_group['Q4-2']
        annual_rpt_data = pd.concat([annual_rpt_data, year_group], axis=1)
        
        dat_grouped = pd.concat([datdf.reindex(month_group.index), month_group['Q4-1']], axis=1)
        
        res = pd.DataFrame()
        for gp, df in dat_grouped.groupby('Q4-1'):
            data_to_broadcast = annual_rpt_data.loc[annual_rpt_data['Q4-2']==gp].iloc[:,:-1]
            tmp = pd.DataFrame(index=df.index, columns=df.columns[:-1])
            if len(data_to_broadcast) == 0:   
                tmp.loc[:,:] = np.nan
            else:
                tmp.loc[:,:] = np.repeat(data_to_broadcast.values, len(tmp), 0)
            res = pd.concat([res, tmp])                
        res = res.sort_index()
        return res.T
    
    def update_real_rptdate(self, freq='M'):
        idate = self.stm_issuingdate
        delist_map = self.meta['delist_date'].to_dict()
        if freq == 'M':
            tdates = self.month_map.values
        elif freq == 'd':
            tdates = self.close.columns
        
        applied_rpt_date = pd.DataFrame(columns=idate.index, index=tdates)
        applied_rpt_date = applied_rpt_date.apply(self._get_apply_rptdate, 
                                                  args=(idate, delist_map)).T
        self.close_file(applied_rpt_date, f'applied_rpt_date_{freq}')
        print(f"'applied_rpt_date_{freq}' data updated.")
        
    def _align_element(self, df1, df2):
        row_index = sorted(df1.index.intersection(df2.index))
        col_index = sorted(df1.columns.intersection(df2.columns))
        return df1.loc[row_index, col_index], df2.loc[row_index, col_index]
    
    def _get_apply_rptdate(self, df, idate=None, delist_map=None):        
        code = df.name
        delist_date = delist_map[code]
        rptrealdates = idate.loc[code,:].tolist()
        
        if pd.isnull(delist_date): 
            res = [self.__append_date(rptrealdates, curdate, idate) for curdate in df.index]
        else:
            res = []
            for curdate in df.index:
                if curdate >= delist_date:
                    res.append(pd.NaT)
                else:
                    res.append(self.__append_date(rptrealdates, curdate, idate))
        return res
    
    @staticmethod
    def __append_date(rptrealdates, curdate, idate, base_time='1899-12-30 00:00:00'):
        base_time = pd.to_datetime(base_time)
        rptavaildates = sorted(d for d in rptrealdates if d < curdate and d != base_time)
        if rptavaildates:
            availdate1 = rptavaildates[-1]
            didx = rptrealdates.index(availdate1) 
            try:
                availdate2 = rptavaildates[-2]
            except IndexError:
                pass
            else:
               if availdate1 == availdate2:
                   didx += 1
            finally:
               return idate.columns[didx]
        else:
            return pd.NaT 

if __name__ == '__main__':
    w.start()
    updatefreq = input("Choose update frequency between 'w' and 'M': ")
    z = UpdateOriginData(updatefreq, update_only=True)
    z.update_meta_data()
    if updatefreq == 'M':
        z.update_month_map_data()
        z.update_monthly_data()
        z.update_daily_data(include_today=True)
    else:
        z.update_daily_data(include_today=True)
    

import numpy as np
import shutil
from xtquant import xtdatacenter as xtdc
from xtquant import xtdata
import os
import pandas as pd
import datetime
from pathlib import Path
import cfg

def init():
    xtdc.set_token('be43106f3046b74c948b76b9db014a5a460648e4')
    addr_list = [
        '218.16.123.27:55310',
        '218.16.123.86:55310',
        '115.231.218.73:55310',
        '115.231.218.79:55310',
        '36.99.48.20:55300',
        '36.99.48.21:55300',
        '42.228.16.210:55300',
        '42.228.16.211:55300'
    ]
    xtdc.set_allow_optmize_address(addr_list)
    xtdc.init()


# stock_code_list = xtdata.get_stock_list_in_sector('沪深A股')


def get_one_day_data(start_time, dump_path, day_end, stock_list):
    print('downloading for: ', start_time, day_end)
    start_time_str = datetime.datetime.strftime(start_time, '%Y%m%d%H%M%S')
    day_end_str = datetime.datetime.strftime(day_end, '%Y%m%d%H%M%S')
    for stock_code in stock_list:
        xtdata.download_history_data(
            stock_code, "tick", start_time_str, day_end_str)
        res_data = xtdata.get_market_data_ex(stock_list=[stock_code], period='tick', start_time=start_time_str,
                                             end_time=day_end_str, count=-1, dividend_type='front', fill_data=True)
        dump_fn = f"{dump_path}/{stock_code}.csv"
        res_data[stock_code].to_csv(dump_fn, encoding='GB18030')
        print(f"done for {stock_code}")
    print('---------------download done----------------')


def work(start_time, config_path, dump_path):
    start_time = datetime.datetime.strptime(start_time, '%Y%m%d %H:%M:%S')
    start_date_str = datetime.datetime.strftime(start_time, '%Y%m%d')
    dump_path = f"{dump_path}/{start_date_str}"
    if not os.path.exists(dump_path):
        os.makedirs(dump_path)
    day_end = datetime.datetime.now()

    config = pd.read_excel(config_path)
    stock_list = list(set(config["cb_code"].unique())
                      | set(config["stock_code"].unique()))

    get_one_day_data(start_time, dump_path, day_end, stock_list)


def main():
    init()
    start_time = cfg.start_date + " 9:30:00"
    config_path = cfg.config_path
    dump_path = cfg.snapshot_dir
    work(start_time, config_path, dump_path)

if __name__ == '__main__':
    main()
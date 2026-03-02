import os
import numpy as np
import pandas as pd
import ast
from concurrent.futures import ProcessPoolExecutor
import cfg


def align_time(df_time):
    df_seconds = pd.to_timedelta(df_time).dt.total_seconds()
    df_seconds_mod3 = df_seconds % 3
    df_seconds_result = df_seconds.copy()
    df_seconds_result.loc[df_seconds_mod3 < 2] -= df_seconds_mod3
    df_seconds_result.loc[df_seconds_mod3 == 2] += 1
    df_time_aligned = pd.to_timedelta(df_seconds_result, unit='s')
    return df_time_aligned


def calc_price(df_bidprice1, df_askprice1, df_bidvolume1, df_askvolume1):
    invalid_bid = df_bidprice1.isna() | df_bidprice1 == 0
    invalid_ask = df_askprice1.isna() | df_askprice1 == 0
    invalid_rows = invalid_bid | invalid_ask
    df_price = df_bidprice1 + .02
    df_bidvolume1_sqrt = np.sqrt(df_bidvolume1)
    df_askvolume1_sqrt = np.sqrt(df_askvolume1)
    df_price[~invalid_rows] = (df_bidprice1 * df_askvolume1_sqrt + df_askprice1 *
                               df_bidvolume1_sqrt) / (df_bidvolume1_sqrt + df_askvolume1_sqrt)
    df_price[invalid_bid] = df_askprice1 - .02
    df_price = round(df_price, 3)
    return df_price


def generate_prem(args):
    row, convert_px_path, begin_date, snapshot_path, dump_path = args

    convert_px = pd.read_excel(convert_px_path)
    convert_px["dint"] = convert_px["Date"].dt.strftime("%Y%m%d").astype(int)
    convert_px.set_index("dint", inplace=True)

    start_time = pd.to_timedelta("9:30:00")
    end_time = pd.to_timedelta("14:56:57")

    stock_code, cb_code = row["stock_code"], row["cb_code"]
    stock_fn = f"{snapshot_path}/{stock_code}.csv"
    cb_fn = f"{snapshot_path}/{cb_code}.csv"
    
    if cb_code not in convert_px.columns:
        print(f"{cb_code} not found in convert_px")
        return

    if not os.path.exists(stock_fn) or not os.path.exists(cb_fn):
        print(f"Data file not found for {stock_code} and {cb_code}: {stock_fn}, {cb_fn}")
        return

    stock = pd.read_csv(stock_fn).rename(columns={"Unnamed: 0": "datetime"})
    stock["datetime"] = pd.to_datetime(stock["datetime"], format="%Y%m%d%H%M%S")
    stock["dint"] = stock["datetime"].dt.strftime("%Y%m%d").astype(int)
    stock["convert_price"] = stock["dint"].map(convert_px[cb_code])
    stock["time"] = pd.to_timedelta(stock["datetime"].dt.time.astype(str))
    stock["time"] = align_time(stock["time"])
    stock = stock[(start_time <= stock["time"]) & (stock["time"] <= end_time)]
    stock["bidPrice1"] = round(stock["bidPrice"].apply(ast.literal_eval).apply(lambda x: x[0]), 2)
    stock["askPrice1"] = round(stock["askPrice"].apply(ast.literal_eval).apply(lambda x: x[0]), 2)
    stock["bidVolume1"] = stock["bidVol"].apply(ast.literal_eval).apply(lambda x: x[0])
    stock["askVolume1"] = stock["askVol"].apply(ast.literal_eval).apply(lambda x: x[0])
    stock["stock_price"] = calc_price(stock["bidPrice1"], stock["askPrice1"], stock["bidVolume1"], stock["askVolume1"])
    
    bond = pd.read_csv(cb_fn).rename(columns={"Unnamed: 0": "datetime"})
    bond["datetime"] = pd.to_datetime(bond["datetime"], format="%Y%m%d%H%M%S")
    bond["dint"] = bond["datetime"].dt.strftime("%Y%m%d").astype(int)
    bond["time"] = pd.to_timedelta(bond["datetime"].dt.time.astype(str))
    bond["time"] = align_time(bond["time"])
    bond = bond[(start_time <= bond["time"]) & (bond["time"] <= end_time)]
    bond["bidPrice5"] = round(bond["bidPrice"].apply(ast.literal_eval).apply(lambda x: x[4]), 3)
    bond["askPrice5"] = round(bond["askPrice"].apply(ast.literal_eval).apply(lambda x: x[4]), 3)

    merge=pd.merge(stock[["dint", "time", "stock_price"]], bond[["dint", "time", "bidPrice5", "askPrice5"]], on=["dint", "time"])
    merge["convert_price"] = merge["dint"].map(convert_px[cb_code])
    merge['prem_A']=(merge["bidPrice5"]-merge["stock_price"]*100/merge["convert_price"])/merge["bidPrice5"]*10000
    merge['prem_B']=(merge["askPrice5"]-merge["stock_price"]*100/merge["convert_price"])/merge["askPrice5"]*10000
    merge.drop_duplicates(inplace=True)
    merge.dropna(inplace=True)
    merge = merge[merge["bidPrice5"] != 0]
    merge.to_csv(f"{dump_path}/{begin_date}_{cb_code[0:6]}_prem.csv", index=False)


def multi_generate_prem(config_path, convert_px_path, begin_date, snapshot_path, dump_path):
    if not os.path.exists(dump_path):
        os.makedirs(dump_path)
    config = pd.read_excel(config_path)

    args_list = []
    for i, row in config.iterrows():
        args = (row, convert_px_path, begin_date, snapshot_path, dump_path)
        generate_prem(args)
    #     args_list.append(args)
    # with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
    #     list(executor.map(generate_prem, args_list))

def main():
    config_path = cfg.config_dir + 'cb_filter.xlsx'
    convert_px_path = cfg.config_dir + "convert_px.xlsx"
    begin_date = cfg.start_date
    snapshot_path = f"{cfg.snapshot_dir}/{begin_date}"
    dump_path = f"{cfg.prem_dir}/{begin_date}"
    multi_generate_prem(config_path, convert_px_path, begin_date, snapshot_path, dump_path)
    print("prem finished")

if __name__ == '__main__':
    main()
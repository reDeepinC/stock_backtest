import pandas as pd
import cfg

def main():
    name_path = cfg.config_path
    summary_path = cfg.result_path
    output_path = name_path.replace('.xlsx', '_new.xlsx')
    
    df1 = pd.read_excel(name_path)
    if name_path.find('name_wu') != -1:
        df1 = df1[df1.columns[1:]]
    df2 = pd.read_csv(summary_path)
    
    df1["_cb6"] = df1["cb_code"].str[:6]
    df2["cb_code"] = df2["cb_code"].astype(str)
    df1 = df1.merge(
        df2[["cb_code", "optimal_k", "optimal_timing_interval"]],
        left_on="_cb6",
        right_on="cb_code",
        how="left",
        sort=False,
        validate="m:1"
    )
    df1["buy_edge_prem_k"]  = df1["optimal_k"]
    df1["sell_edge_prem_k"] = df1["optimal_k"]
    df1["row_count"]        = df1["optimal_timing_interval"]
    df1.drop(columns=["_cb6", "cb_code_y", "optimal_k", "optimal_timing_interval"], inplace=True)
    df1.rename(columns={"cb_code_x": "cb_code"}, inplace=True)
    df1.to_excel(output_path, index=False)

if __name__ == '__main__':
    main()
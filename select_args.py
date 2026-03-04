import pandas as pd
import cfg

def main():
    names = pd.read_excel(cfg.config_path, index_col='cb_code')
    dfs = []
    for cb in names.index:
        df = pd.read_csv(f'search/single_stock_search_{cfg.start_date}_{cb[:-3]}_prem.csv', usecols=['k', 'timing_interval', 'final_return_pct', 'trade_count'])
        df['cb'] = cb
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df = df.groupby(['k', 'timing_interval']).agg(avg_return=('final_return_pct', 'mean'), trade_symbols=('trade_count', lambda x: df.loc[x.index, 'cb'][x > 0].nunique()), trade_count=('trade_count', 'sum')).reset_index().sort_values(by='avg_return', ascending=False)
    print(df)
    df.to_csv('best_args.csv')

if __name__ == '__main__':
    main()
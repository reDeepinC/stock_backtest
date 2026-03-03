import pandas as pd
import numpy as np
import os
import glob
from datetime import time as dtime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import warnings
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端，避免多进程环境中的tkinter错误
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import math
import csv
from openpyxl import load_workbook  
warnings.filterwarnings('ignore')
import cfg

# 获取脚本所在目录，用于统一图片保存路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def calcute_prem(prem,base_prem,sigma_prem,max_inv,k):
    """使用跳界函数"""
    u = base_prem
    sigma = sigma_prem
    prem_bid = np.maximum(u + k * sigma,22.5)
    prem_ask=u-k*sigma
    return prem_ask,prem_bid
    

def run_advanced_backtest(df, strategy='jump', k=3,timing_interval=600,date=None):
    """
    运行高级回测逻辑，包含交易记录
    
    Args:
        df: 数据DataFrame
        strategy: 策略类型 ('linear', 'multilinear', 'sigmoid')
        
    Returns:
        回测结果字典
    """
    try:
        # 按时间排序并重置索引，确保索引从0开始连续
        df.sort_values(by=['datetime'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        df['prem']=(df['prem_A']+df['prem_B'])/2
        # 计算滚动基准premium
        df['base_prem'] = df['prem'].ewm(span=timing_interval, adjust=False).mean()
        df['base_vol'] = df['prem'].rolling(window=timing_interval).std()
        df['prem_ask'],df['prem_bid'] = calcute_prem(df['prem'],df['base_prem'],df['base_vol'],100,k=k)
   

        # 初始化变量
        df['S'] = 100  # 股票持仓
        df['dollar_volume'] = 100 * df['stock_price']  # 总资金
        df['S_sellable'] = 0  # 可卖股票数量
        df['total_cash'] = 0  # 累计现金
        df['trade'] = 0  # 交易标记
        # 初始化第一行
        df.loc[0, 'S'] = 100
        df.loc[0, 'C'] = 0
        df.loc[0, 'cash']=0
        
        
        profits = []  # 记录每笔交易的利润
        trade_records = []  # 记录每笔交易的详细信息
        trade_count = 0  # 交易次数
        prem_before=0
        for i in range(1, len(df)):
            c_dint = df.loc[i, 'dint']
            c_time = df.loc[i, 'time']
            #更新S_sellable
            if df.loc[i, 'dint'] != df.loc[i-1, 'dint']:
                # 新的一天开始时，S_sellable设置为前一天最后的S值
                if (df.loc[i-1,'prem_B'] < -10) and (df.loc[i-1,'C'] == 0) and(df.loc[i-1,'S_sellable'] >0): # 收盘转股套利
                    df.loc[i-1,'cash'] += (-df.loc[i-1,'prem_B']-5.854)/10000 * df.loc[i-1,'C']*df.loc[i-1,'bidPrice5']# 尾盘卖股买债转股
                if (df.loc[i-1,'prem_B'] < 0) and (df.loc[i-1,'C'] > 0): # 转股逻辑
                    df.loc[i-1, 'S'] = 100
                    df.loc[i-1,'S_sellable']=100
                    df.loc[i-1,'C'] =0
                df.loc[i, 'S_sellable'] = df.loc[i - 1, 'S']

            else:
                # 同一天内，S_sellable保持不变
                # 注意：执行卖股买债后，S_sellable被设置为0；执行卖债买股后，S_sellable保持为0
                # 新获得的股票只能在第二天卖出
                df.loc[i, 'S_sellable'] = df.loc[i-1, 'S_sellable']
            df.loc[i,'cash'] = df.loc[i-1,'cash']
            df.loc[i,'S'] = df.loc[i-1, 'S']
            df.loc[i,'C'] = df.loc[i-1, 'C']

            #执行卖股买债
            if (df.loc[i,'S_sellable']>0) & (df.loc[i,'prem_B'] < df.loc[i,'prem_ask']):
                sell_volume=df.loc[i,'S_sellable'] * df.loc[i,'stock_price']
                #buy_volume=df.loc[i,'dollar_volume']
                buy_volume = df.loc[i, 'S'] * df.loc[i, 'stock_price'] / (1 - df.loc[i, 'prem_ask'] / 10000)
                trade_cost=sell_volume*5.854 / 10000+buy_volume*0.5 / 10000
                df.loc[i,'S']=0
                df.loc[i,'C']=buy_volume/df.loc[i,'askPrice5']
                df.loc[i,'cash']=df.loc[i,'cash']+sell_volume-buy_volume-trade_cost
                df.loc[i,'S_sellable']=0  # 执行卖股买债后，S_sellable设置为0，同一天内不再更新
                # print(f"{df.loc[i, 'datetime']},在prem={df.loc[i,'prem']},short prem,卖股买债")
                df.loc[i, 'trade'] = 1
                trade_count += 1
                # 记录交易详情
                trade_records.append({
                    'datetime': df.loc[i, 'datetime'],
                    'dint': c_dint,
                    'time': c_time,
                    'index': i,
                    'prem': max(df.loc[i,'prem_B'],df.loc[i-1,'prem_B']),
                    'stock_price': df.loc[i, 'stock_price'],
                    'cb_price': df.loc[i, 'askPrice5'],
                    'trade_type': 'BUY',  # 买股卖债
                    'trade_volume': sell_volume+buy_volume,
                })
                continue
                
            #执行卖债买股
            elif (df.loc[i,'S_sellable']==0)&(df.loc[i,'S']==0)&(df.loc[i,'prem_A']>df.loc[i,'prem_bid']):
                sell_volume=df.loc[i,'C']*df.loc[i,'bidPrice5']
                buy_volume=df.loc[i,'stock_price'] * 100
                trade_cost=sell_volume*0.5 / 10000+buy_volume*0.854 / 10000
                df.loc[i,'S']=buy_volume/df.loc[i,'stock_price']
                df.loc[i,'C']=0
                df.loc[i,'cash']=df.loc[i,'cash']+sell_volume-buy_volume-trade_cost
                # 卖债买股后，S_sellable在同一天内不再更新，保持为0，防止同一天内再次卖股买债
                df.loc[i,'S_sellable']=0
                # print(f"{df.loc[i, 'datetime']},在prem={df.loc[i,'prem']},long prem,卖债买股")
                df.loc[i, 'trade'] = -1
                trade_count += 1
                # 记录交易详情
                trade_records.append({
                    'datetime': df.loc[i, 'datetime'],
                    'dint': c_dint,
                    'time': c_time,
                    'index': i,
                    'prem': min(df.loc[i,'prem_A'],df.loc[i-1,'prem_A']),
                    'stock_price': df.loc[i, 'stock_price'],
                    'cb_price': df.loc[i, 'bidPrice5'],
                    'trade_type': 'SELL',  # 买股卖债
                    'trade_volume': sell_volume+buy_volume,
                })
                continue
            else:
                continue
        
        df['total_cash']=df['cash']+df['S']*df['stock_price']+df['C']*df['bidPrice5']-df['stock_price']*100 # 超额收益
        # 计算最终收益率
        final_return = (df['total_cash'].values[-1] / (df['stock_price'].values[0]*100))*100
        # 计算回测天数
        days = df['dint'].unique().size
        # 计算年化收益率
        if days > 0:
            annual_return = (1 + final_return / 100) ** (252 / days) - 1
            annual_return_pct = annual_return * 100
        else:
            annual_return_pct = np.nan
        # 计算其他指标
        total_profit = df['total_cash'].values[-1]
        # 计算最大回撤
        cumulative_cash = df['total_cash'].values
        running_max = np.maximum.accumulate(cumulative_cash)
        drawdown = cumulative_cash - running_max
        max_drawdown = np.min(drawdown)
        

        code_name = df['cb_code'].values[0]
        dint = df['dint'].values[0]
        save_dir = rf'marketmaking/trade_record/backtest_record/{dint}'

        prem_edge=df[df['trade']==1]['prem'].mean()-df[df['trade']==-1]['prem'].mean()
        # 先确保目录存在
        os.makedirs(save_dir, exist_ok=True)

        save_path = rf'{save_dir}/{code_name}.csv'
        df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        return {
            'final_return_pct': annual_return_pct,
            'total_profit': total_profit,
            'trade_count': trade_count,
            'max_drawdown': max_drawdown,
            'final_inventory': df['S'].iloc[-1],
            'avg_inventory': df['S'].mean(),
            'inventory_volatility': df['S'].std(),
            'data_points': len(df),
            'start_time': df['datetime'].iloc[0],
            'end_time': df['datetime'].iloc[-1],
            'prem_edge': prem_edge,
            'trade_records': trade_records,  # 添加交易记录
            'df': df  # 添加处理后的数据框用于绘图
        }
        
    except Exception as e:
        print(f"回测过程中出错: {e}")
        import traceback
        traceback.print_exc()
        return {'error': str(e)}

def plot_trades_with_premium(df, trade_records, file_name, strategy, save_path=None):
    """
    绘制premium时间序列图和交易点,同时绘制total_cash(双坐标轴)

    Args:
        df: 数据DataFrame
        trade_records: 交易记录列表
        file_name: 文件名
        strategy: 策略名称
        save_path: 保存路径
    """
    try:
        fig, ax1 = plt.subplots(figsize=(15, 10))

        # 使用连续的索引作为x轴，确保时间连续
        x_axis = range(len(df))
        x_label = 'Time Index'

        # 绘制premium时间序列
        ax1.plot(x_axis, df['prem'], 'b-', alpha=0.7, linewidth=1, label='Premium')
        # 绘制base_prem线
        ax1.plot(x_axis, df['base_prem'], 'g-', alpha=1, linewidth=1, label='Base Premium')
        ax1.set_xlabel(x_label, fontsize=12)
        ax1.set_ylabel('Premium', color='b', fontsize=12)
        ax1.tick_params(axis='y', labelcolor='b')
        ax1.legend(loc='upper left')

        # 创建第二个Y轴
        ax2 = ax1.twinx()
        
        # 绘制总资产曲线（total_cash + 初始资金）
        initial_capital = df['dollar_volume'].values[0]
        ax2.plot(x_axis, df['total_cash']/df['dollar_volume'], 'r-', alpha=0.7, linewidth=1, label='profit ratio')
        # 绘制初始资金基准线
        ax2.set_ylabel('Total Asset', color='r', fontsize=12)
        ax2.tick_params(axis='y', labelcolor='r')
        ax2.legend(loc='upper right')

        # 绘制交易点
        buy_trades = [t for t in trade_records if t['trade_type'] in ['BUY']]
        sell_trades = [t for t in trade_records if t['trade_type'] in ['SELL']]

        if buy_trades:
            # 使用交易记录的index作为x坐标
            buy_x = [t['index'] for t in buy_trades]
            buy_y = [t['prem'] for t in buy_trades]
            # 使用固定大小
            ax1.scatter(buy_x, buy_y, c='red', s=50, alpha=0.7, label=f'Buy ({len(buy_trades)} trades)', zorder=5)

        if sell_trades:
            # 使用交易记录的index作为x坐标
            sell_x = [t['index'] for t in sell_trades]
            sell_y = [t['prem'] for t in sell_trades]
            # 使用固定大小
            ax1.scatter(sell_x, sell_y, c='green', s=50, alpha=0.7, label=f'Sell ({len(sell_trades)} trades)', zorder=5)

        # 设置x轴格式，显示时间标签
        if 'datetime' in df.columns:
            # 计算合适的时间间隔
            total_points = len(df)
            if total_points <= 50:
                interval = 1
            elif total_points <= 200:
                interval = max(1, total_points // 20)
            else:
                interval = max(1, total_points // 30)
            
            # 设置x轴刻度位置
            tick_positions = list(range(0, total_points, interval))
            if total_points - 1 not in tick_positions:
                tick_positions.append(total_points - 1)
            
            # 设置x轴刻度标签
            tick_labels = []
            for pos in tick_positions:
                if pos < len(df):
                    # 显示时间+日期（几号）
                    time_str = df['datetime'].iloc[pos].strftime('%H:%M:%S:%m:/%d')
                    tick_labels.append(time_str)
                else:
                    tick_labels.append('')
            
            ax1.set_xticks(tick_positions)
            ax1.set_xticklabels(tick_labels, rotation=45, ha='right')
            
            # 设置x轴标签
            ax1.set_xlabel('Time', fontsize=12)
        else:
            # 如果没有datetime列，使用索引
            ax1.set_xlabel('Time Index', fontsize=12)

        # 设置图表属性
        plt.title(f'Premium and Total Cash Time Series with Trades - {file_name} ({strategy.upper()})', fontsize=14)
        plt.grid(True, alpha=0.3)

        # 调整布局
        fig.tight_layout()

        # 保存图片
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"交易图已保存到: {save_path}")

    except Exception as e:
        print(f"绘制交易图时出错: {e}")


def process_single_file(file_path, strategy='jump', k=3, plot_trades=True,timing_interval=600,date=None):
    """
    处理单个文件的回测
    
    Args:
        file_path: CSV文件路径
        strategy: 策略类型 ('linear', 'multilinear', 'sigmoid')
        plot_trades: 是否绘制交易图
        
    Returns:
        回测结果字典
    """
    try:
        # 获取文件名
        file_name = os.path.basename(file_path).replace('.csv', '')
        # 读取数据
        df = pd.read_csv(file_path)
        
        # 检查必要的列是否存在
        required_columns = ['dint', 'time', 'stock_price', 'bidPrice5', 'askPrice5', 'prem_A','prem_B']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"缺少必要的列: {missing_columns}")
            return {'file_name': file_name, 'error': f'缺少必要的列: {missing_columns}'}
        
        # 数据类型转换和清理
        df['prem']=(df['prem_B']+df['prem_A'])/2
        df['dint'] = pd.to_numeric(df['dint'], errors='coerce')
        df['stock_price'] = pd.to_numeric(df['stock_price'], errors='coerce')
        # df['cb_price'] = pd.to_numeric(df['cb_price'], errors='coerce')
        df['prem'] = pd.to_numeric(df['prem'], errors='coerce')
        
        # 移除无效数据
        df = df.dropna(subset=['dint', 'time', 'stock_price', 'bidPrice5', 'askPrice5', 'prem_A','prem_B'])
        
        df['base_vol']=df['prem'].rolling(window=timing_interval).std()
        df['base_prem']=df['prem'].ewm(span=timing_interval,adjust=False).mean()
        df.dropna(inplace=True)
        df=df.merge(df.groupby('dint')['base_vol'].mean().shift(1).reset_index().rename(columns={'base_vol':'last_base_vol_mean'}),on='dint',how='left')
        df.fillna(0,inplace=True)
        df['base_vol']=df.apply(lambda x:x['last_base_vol_mean'] if x['base_vol']<x['last_base_vol_mean'] else x['base_vol'],axis=1)

        df['cb_code']=file_name.split('_')[1]
        
        # 处理时间列
        df['datetime_str'] = df['dint'].astype(str) + ' ' + df['time'].str[-8:]
        df['datetime'] = pd.to_datetime(df['datetime_str'], format='%Y%m%d %H:%M:%S')
        df.drop(columns=['datetime_str'], inplace=True)
    
        
        # 设置时间过滤条件
        cutoff_time = dtime(14, 56, 57)
        begin_time = dtime(9, 30, 9)
        
        # 转换时间格式
        df['time'] = df['time'].astype(str).str[-8:]
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
        
        # 过滤时间范围
        df = df[(df['time'] <= cutoff_time) & (df['time'] >= begin_time)].reset_index(drop=True).copy()
        
        if df.empty:
            return {'file_name': file_name, 'error': '数据为空'}
        
        # 运行高级回测
        result = run_advanced_backtest(df, strategy, k,timing_interval)
        
        if 'error' in result:
            return {'file_name': file_name, 'error': result['error']}
        
        # 添加文件信息
        result['file_name'] = file_name
        result['strategy'] = strategy
        
        # 绘制交易图
        if plot_trades and result['trade_records']:
            # 创建图表保存目录（基于脚本目录）
            plot_dir = os.path.join(SCRIPT_DIR, f"trade_plots_{strategy}")
            os.makedirs(plot_dir, exist_ok=True)
            plot_path = os.path.join(plot_dir, f"{file_name}_trades.png")
            
            plot_trades_with_premium(
                result['df'], 
                result['trade_records'], 
                file_name, 
                strategy, 
                save_path=plot_path
            )
        
        # 实时写入Excel
        csv_file = r'marketmaking/premium/stocks_backtest_results.xlsx'
        if not os.path.exists("marketmaking/premium"):
            os.makedirs("marketmaking/premium")
        csv_columns = ['cb_code', 'final_return_pct', 'trade_count']

        # 检查文件是否存在，决定是否写表头
        write_header = not os.path.exists(csv_file)

        csv_row = {
            'cb_code': file_name.split('_')[1],
            'final_return_pct': result.get('final_return_pct', ''),
            'trade_count': result.get('trade_count', ''),
        }

        with open(csv_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=csv_columns)
            if write_header:
                writer.writeheader()
            writer.writerow(csv_row)
        return result

    
        
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")
        return {'file_name': os.path.basename(file_path).replace('.csv', ''), 'error': str(e)}

def run_parallel_backtest(data_dir, strategies=['jump'], 
                         k=3, max_workers=None, plot_trades=True,timing_interval=600,date=None):
    """
    运行并行回测
    
    Args:
        data_dir: 数据目录路径
        strategies: 要测试的策略列表
        max_workers: 最大工作进程数
        plot_trades: 是否绘制交易图
        
    Returns:
        所有回测结果的列表
    """
    # 获取所有CSV文件
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    if not csv_files:
        print(f"在目录 {data_dir} 中未找到CSV文件")
        return []
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    
    # 设置工作进程数
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(csv_files) * len(strategies))
    
    print(f"使用 {max_workers} 个工作进程")
    
    all_results = []
    
    # 为每个策略运行回测
    for strategy in strategies:
        print(f"\n开始测试策略: {strategy}")
        
        # 使用进程池并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_file = {
                executor.submit(process_single_file, file_path, strategy='jump', k=k, plot_trades=True,timing_interval=timing_interval,date=None): file_path 
                for file_path in csv_files
            }
            
            # 收集结果
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    print(f"处理文件 {file_path} 时发生异常: {e}")
                    all_results.append({
                        'file_name': os.path.basename(file_path).replace('.csv', ''),
                        'strategy': strategy,
                        'error': str(e)
                    })
    
    return all_results

def analyze_results(results):
    """分析回测结果"""
    if not results:
        print("没有结果可分析")
        return
    
    strategies = {}
    for result in results:
        if 'error' in result:
            continue
        strategy = result['strategy']
        if strategy not in strategies:
            strategies[strategy] = []
        strategies[strategy].append(result)
    
    print("\n" + "="*80)
    print("回测结果汇总")
    print("="*80)
    
    for strategy, strategy_results in strategies.items():
        if not strategy_results:
            continue
            
        print(f"\n策略: {strategy.upper()}")
        print("-" * 50)
        
        # 计算汇总统计
        returns = [r['final_return_pct'] for r in strategy_results if 'final_return_pct' in r]
        trade_counts = [r['trade_count'] for r in strategy_results if 'trade_count' in r]
        if returns:
            print(f"收益率统计:")
            print(f"  平均: {np.mean(returns):.2f}%")
            print(f"  中位数: {np.median(returns):.2f}%")
            print(f"  标准差: {np.std(returns):.2f}%")
            print(f"  最小值: {np.min(returns):.2f}%")
            print(f"  最大值: {np.max(returns):.2f}%")
            print(f"  正收益文件数: {sum(1 for ret in returns if ret > 0)}/{len(returns)}")
        
        if trade_counts:
            print(f"交易次数统计:")
            print(f"  平均: {np.mean(trade_counts):.1f}")
            print(f"  中位数: {np.median(trade_counts):.1f}")
            print(f"  总交易次数: {np.sum(trade_counts)}")
    
    # 策略比较
    print("\n" + "="*80)
    print("策略比较")
    print("="*80)
    
    strategy_summary = {}
    for strategy, strategy_results in strategies.items():
        if not strategy_results:
            continue
        returns = [r['final_return_pct'] for r in strategy_results if 'final_return_pct' in r]
        if returns:
            strategy_summary[strategy] = {
                'avg_return': np.mean(returns),
                'avg_trade_count': np.mean([r['trade_count'] for r in strategy_results if 'trade_count' in r]),
                'file_count': len(strategy_results)
            }
    
    # 按平均收益率排序
    sorted_strategies = sorted(strategy_summary.items(), key=lambda x: x[1]['avg_return'], reverse=True)
    
    for i, (strategy, metrics) in enumerate(sorted_strategies):
        print(f"{i+1}. {strategy.upper()}:")
        print(f"平均收益率: {metrics['avg_return']:.2f}%")
        print(f"平均交易次数: {metrics['avg_trade_count']:.1f}")
        print(f"文件数量: {metrics['file_count']}")

def calculate_equal_weight_pnl(results):
    """
    计算等权持有所有股债的pnl曲线（按天计算）
    
    计算步骤：
    1. 对每个股债，按dint分组，计算每天的收益率 = (当天最后的total_cash - 前一天最后的total_cash) / dollar_volume[0]
    2. 在每天，对所有股债的收益率取均值，得到组合收益率
    3. 使用 (1 + 组合收益率).cumprod() 计算累计PnL
    
    Args:
        results: 回测结果列表，每个结果包含'df'字段
        
    Returns:
        pnl_series: 等权平均的pnl时间序列（按天）
        dint_series: 对应的日期序列（dint）
    """
    valid_results = [r for r in results if 'df' in r and 'error' not in r]
    if not valid_results:
        return None, None
    
    # 收集所有股债的日收益率数据
    all_daily_return_dfs = []
    
    for result in valid_results:
        df = result['df'].copy()
        if 'total_cash' in df.columns and 'dollar_volume' in df.columns and 'dint' in df.columns:
            initial_dollar_volume = df['dollar_volume'].values[0]
            
            # 按dint分组，计算每天的收益率
            daily_returns = []
            daily_dints = []
            
            # 按dint排序
            df = df.sort_values('dint').reset_index(drop=True)
            
            # 获取所有唯一的dint
            unique_dints = sorted(df['dint'].unique())
            
            # 记录前一天的total_cash（初始为0，因为total_cash是相对于初始资金的利润）
            prev_total_cash = 0
            
            for dint in unique_dints:
                # 获取当天的数据
                day_data = df[df['dint'] == dint]
                if len(day_data) == 0:
                    continue
                
                # 当天的最后total_cash
                day_end_total_cash = day_data['total_cash'].iloc[-1]
                
                # 计算当天的收益率 = (当天最后的total_cash - 前一天最后的total_cash) / dollar_volume[0]
                daily_return = (day_end_total_cash - prev_total_cash) / initial_dollar_volume
                if daily_return<-0.1:
                    print(day_data)
                
                daily_returns.append(daily_return)
                daily_dints.append(dint)
                
                # 更新前一天的total_cash
                prev_total_cash = day_end_total_cash
                
            
            # 创建日收益率DataFrame
            daily_return_df = pd.DataFrame({
                'dint': daily_dints,
                'return': daily_returns
            })
            all_daily_return_dfs.append(daily_return_df)
    
    if not all_daily_return_dfs:
        return None, None
    
    # 找到所有日期的并集
    all_dints = set()
    for daily_return_df in all_daily_return_dfs:
        all_dints.update(daily_return_df['dint'].values)
    all_dints = sorted(list(all_dints))
    
    # 创建统一的日期索引DataFrame
    unified_df = pd.DataFrame({'dint': all_dints})
    unified_df = unified_df.sort_values('dint').reset_index(drop=True)
    
    # 将每个股债的日收益率数据合并到统一的日期索引上
    for i, daily_return_df in enumerate(all_daily_return_dfs):
        daily_return_df = daily_return_df.copy()
        daily_return_df = daily_return_df.sort_values('dint').reset_index(drop=True)
        # 重命名return列为带索引的列名
        daily_return_df = daily_return_df.rename(columns={'return': f'return_{i}'})
        # 合并到统一DataFrame
        unified_df = unified_df.merge(
            daily_return_df[['dint', f'return_{i}']], 
            on='dint', 
            how='left'
        )
        # 不填充NaN，保留原始数据状态
    
    # 只保留有足够数据点的日期（至少有一半的股债有实际数据）
    return_columns = [col for col in unified_df.columns if col.startswith('return_')]
    min_valid_count = len(return_columns) // 2
    valid_mask = unified_df[return_columns].notna().sum(axis=1) >= min_valid_count
    unified_df = unified_df[valid_mask]
    
    # 在每天，计算所有股债收益率的均值，得到组合收益率（只考虑有数据的股债）
    unified_df['portfolio_return'] = unified_df[return_columns].mean(axis=1, skipna=True)
    
    # 计算组合pnl = (1 + 组合收益率)的累乘
    unified_df['equal_weight_pnl'] = (1 + unified_df['portfolio_return']).cumprod()
    
    return unified_df['equal_weight_pnl'].values, unified_df['dint'].values


def plot_equal_weight_pnl(pnl_series, dint_series, k, timing_interval, save_path=None):
    """
    绘制等权平均pnl曲线（按天）
    
    Args:
        pnl_series: pnl时间序列
        dint_series: 对应的日期序列（dint格式，如20251001）
        k: k参数
        timing_interval: timing_interval参数
        save_path: 保存路径
    """
    try:
        fig, ax = plt.subplots(figsize=(15, 8))
        
        # 将dint转换为日期格式
        # 确保dint_series是数组格式
        if isinstance(dint_series, (list, np.ndarray)):
            if len(dint_series) > 0:
                if isinstance(dint_series[0], (int, float, np.integer)):
                    # dint是整数格式，如20251001
                    date_series = pd.to_datetime(dint_series.astype(str), format='%Y%m%d')
                else:
                    date_series = pd.to_datetime(dint_series)
            else:
                date_series = pd.to_datetime([])
        else:
            date_series = pd.to_datetime(dint_series)
        
        
        # 将date_series转换为列表以便处理
        date_list = list(date_series) if hasattr(date_series, '__iter__') and not isinstance(date_series, str) else date_series
        num_points = len(date_list)
        
        # 使用索引位置作为x轴，这样刻度就是等间距的
        x_indices = list(range(num_points))
        
        # 绘制pnl曲线（使用索引位置作为x轴）
        ax.plot(x_indices, pnl_series, 'b-', linewidth=2, marker='o', markersize=4, label='Equal Weight Portfolio PnL')
        ax.axhline(y=1.0, color='r', linestyle='--', alpha=0.5, label='Baseline (1.0)')
        
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Portfolio PnL (1 + Cumulative Return)', fontsize=12)
        ax.set_title(f'Equal Weight Portfolio PnL Curve (Daily)\nk={k}, timing_interval={timing_interval}', fontsize=14)
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        
        # 格式化x轴日期显示 - 只显示有数据的交易日，且刻度等间距
        # 根据数据点数量调整日期显示间隔
        if num_points <= 10:
            # 如果数据点少于10个，显示所有日期
            tick_indices = list(range(num_points))
        elif num_points <= 30:
            # 如果数据点在10-30个之间，每隔几个显示一个
            interval = max(1, num_points // 10)
            tick_indices = list(range(0, num_points, interval))
            # 确保最后一个日期也被显示
            if num_points > 0 and (num_points - 1) not in tick_indices:
                tick_indices.append(num_points - 1)
        else:
            # 如果数据点超过30个，每隔更多个显示一个
            interval = max(1, num_points // 15)
            tick_indices = list(range(0, num_points, interval))
            # 确保最后一个日期也被显示
            if num_points > 0 and (num_points - 1) not in tick_indices:
                tick_indices.append(num_points - 1)
        
        # 使用索引位置作为刻度位置（等间距），标签显示对应的日期
        tick_positions = tick_indices
        # 标签只显示有数据的交易日
        tick_labels = [date_list[i].strftime('%Y-%m-%d') for i in tick_indices]
        
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=45, ha='right')
        
        # 显示最终收益
        final_pnl = pnl_series[-1] if len(pnl_series) > 0 else 1.0
        final_return = (final_pnl - 1) * 100
        ax.text(0.02, 0.98, f'Final PnL: {final_pnl:.4f}\nPortfolio Return: {final_return:.2f}%', 
                transform=ax.transAxes, fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        fig.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"等权PnL曲线已保存到: {save_path}")
        else:
            plt.show()
        
        plt.close()
        
    except Exception as e:
        print(f"绘制等权PnL曲线时出错: {e}")


def single_stock_parameter_search_args(args):
    """
    对单个股票进行参数搜索，找到最优参数组合
    
    Args:
        file_path: 单个CSV文件路径
        k_list: k参数列表
        timing_interval_list: timing_interval参数列表
        strategy: 策略类型
        plot_trades: 是否绘制交易图（仅对最优参数）
        save_results: 是否保存结果
        parallel: 是否并行处理参数组合（True=并行处理参数组合，False=依次处理）
        max_workers: 并行时的最大工作进程数
        
    Returns:
        包含所有参数组合结果和最优参数的字典
    """
    file_path, k_list, timing_interval_list, strategy, plot_trades, save_results, parallel, max_workers = args
    
    all_results = []
    best_result = None
    best_return = float('-inf')
    
    total_combinations = len(k_list) * len(timing_interval_list)
    
    # 生成所有参数组合
    param_combinations = [(k, timing_interval) for k in k_list for timing_interval in timing_interval_list]
    
    # 依次测试所有参数组合
    current_combination = 0
    for k, timing_interval in param_combinations:
        current_combination += 1
        
        # 运行单个文件的回测
        result = process_single_file(
            file_path=file_path,
            strategy=strategy,
            k=k,
            plot_trades=False,  # 先不绘图，最后只对最优参数绘图
            timing_interval=timing_interval,
            date=None
        )
        
        # 检查是否有错误
        if 'error' in result:
            print(f"  {file_path} k={k} timing_interval={timing_interval} 错误: {result['error']}")
            all_results.append({
                'k': k,
                'timing_interval': timing_interval,
                'final_return_pct': np.nan,
                'trade_count': 0,
                'max_drawdown': np.nan,
                'prem_edge': np.nan,
                'error': result['error']
            })
            continue
        
        # 获取收益率
        return_pct = result.get('final_return_pct', np.nan)
        trade_count = result.get('trade_count', 0)
        max_drawdown = result.get('max_drawdown', np.nan)
        prem_edge = result.get('prem_edge', np.nan)
        
        # 保存结果
        param_result = {
            'k': k,
            'timing_interval': timing_interval,
            'final_return_pct': return_pct,
            'trade_count': trade_count,
            'max_drawdown': max_drawdown,
            'prem_edge': prem_edge,
            'total_profit': result.get('total_profit', np.nan),
            'final_inventory': result.get('final_inventory', np.nan),
            'avg_inventory': result.get('avg_inventory', np.nan),
            'inventory_volatility': result.get('inventory_volatility', np.nan),
            'data_points': result.get('data_points', 0)
        }
        all_results.append(param_result)
        
        # 更新最优结果（基于收益率）
        if not np.isnan(return_pct) and return_pct > best_return:
            best_return = return_pct
            best_result = {
                'result': result,
                'k': k,
                'timing_interval': timing_interval,
                'param_result': param_result
            }
    
    # 打印最优参数
    if best_result:        
        print(f"{file_path} 最优参数组合: k={best_result['k']} timing_interval={best_result['timing_interval']} 收益率={best_result['param_result']['final_return_pct']:.2f}% 交易次数={best_result['param_result']['trade_count']} 最大回撤: {best_result['param_result']['max_drawdown']:.2f}")
        
        # 对最优参数绘制交易图
        if plot_trades and best_result['result'].get('trade_records'):
            file_name = os.path.basename(file_path).replace('.csv', '')
            plot_dir = os.path.join(SCRIPT_DIR, f"trade_plots_{strategy}_optimal")
            os.makedirs(plot_dir, exist_ok=True)
            plot_path = os.path.join(plot_dir, f"{file_name}_optimal_k{best_result['k']}_timing{best_result['timing_interval']}.png")
            
            plot_trades_with_premium(
                best_result['result']['df'],
                best_result['result']['trade_records'],
                file_name,
                strategy,
                save_path=plot_path
            )
    else:
        print("未找到有效的最优参数组合")
    
    # 保存结果到CSV
    if save_results:
        results_df = pd.DataFrame(all_results)
        file_name = os.path.basename(file_path).replace('.csv', '')
        save_path = f'search/single_stock_search_{file_name}.csv'
        if not os.path.exists("search"):
            os.makedirs("search")
        results_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        # 保存最优参数摘要
        if best_result:
            summary = {
                'file_name': file_name,
                'optimal_k': best_result['k'],
                'optimal_timing_interval': best_result['timing_interval'],
                'optimal_return_pct': best_result['param_result']['final_return_pct'],
                'optimal_trade_count': best_result['param_result']['trade_count'],
                'optimal_max_drawdown': best_result['param_result']['max_drawdown'],
                'optimal_prem_edge': best_result['param_result']['prem_edge'],
                'total_combinations_tested': total_combinations
            }
            summary_df = pd.DataFrame([summary])
            base_path = r'marketmaking/premium/single_stock_search'
            if not os.path.exists(base_path):
                os.makedirs(base_path)
            summary_path = os.path.join(base_path, f'single_stock_optimal_{file_name}.csv')
            summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
    
    return {
        'all_results': all_results,
        'best_result': best_result,
        'results_df': pd.DataFrame(all_results) if all_results else pd.DataFrame()
    }


def single_stock_parameter_search(
    file_path, k_list, timing_interval_list, 
    strategy='jump', plot_trades=False, save_results=True, parallel=False, max_workers=None
):
    """
    对单个股票进行参数搜索，找到最优参数组合
    
    Args:
        file_path: 单个CSV文件路径
        k_list: k参数列表
        timing_interval_list: timing_interval参数列表
        strategy: 策略类型
        plot_trades: 是否绘制交易图（仅对最优参数）
        save_results: 是否保存结果
        parallel: 是否并行处理参数组合（True=并行处理参数组合，False=依次处理）
        max_workers: 并行时的最大工作进程数
        
    Returns:
        包含所有参数组合结果和最优参数的字典
    """
    
    all_results = []
    best_result = None
    best_return = float('-inf')
    
    total_combinations = len(k_list) * len(timing_interval_list)
    
    # 生成所有参数组合
    param_combinations = [(k, timing_interval) for k in k_list for timing_interval in timing_interval_list]
    
    if parallel:
        # 并行处理参数组合
        if max_workers is None:
            max_workers = min(mp.cpu_count(), total_combinations)
        
        # 准备参数元组列表
        args_list = [
            (file_path, k, timing_interval, strategy)
            for k, timing_interval in param_combinations
        ]
        
        # 并行执行所有参数组合的回测
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_param = {
                executor.submit(process_single_parameter_combination, args): (args[1], args[2])
                for args in args_list
            }
            
            # 收集结果
            completed = 0
            for future in as_completed(future_to_param):
                completed += 1
                k, timing_interval = future_to_param[future]
                try:
                    param_result_dict = future.result()
                    result = param_result_dict['result']
                    
                    # 检查是否有错误
                    if 'error' in result:
                        print(f"  错误: {result['error']}")
                        all_results.append({
                            'k': k,
                            'timing_interval': timing_interval,
                            'final_return_pct': np.nan,
                            'trade_count': 0,
                            'max_drawdown': np.nan,
                            'prem_edge': np.nan,
                            'error': result['error']
                        })
                        continue
                    
                    # 获取收益率
                    return_pct = result.get('final_return_pct', np.nan)
                    trade_count = result.get('trade_count', 0)
                    max_drawdown = result.get('max_drawdown', np.nan)
                    prem_edge = result.get('prem_edge', np.nan)
                    
                    # 保存结果
                    param_result = {
                        'k': k,
                        'timing_interval': timing_interval,
                        'final_return_pct': return_pct,
                        'trade_count': trade_count,
                        'max_drawdown': max_drawdown,
                        'prem_edge': prem_edge,
                        'total_profit': result.get('total_profit', np.nan),
                        'final_inventory': result.get('final_inventory', np.nan),
                        'avg_inventory': result.get('avg_inventory', np.nan),
                        'inventory_volatility': result.get('inventory_volatility', np.nan),
                        'data_points': result.get('data_points', 0)
                    }
                    all_results.append(param_result)
                    
                    # 更新最优结果（基于收益率）
                    if not np.isnan(return_pct) and return_pct > best_return:
                        best_return = return_pct
                        best_result = {
                            'result': result,
                            'k': k,
                            'timing_interval': timing_interval,
                            'param_result': param_result
                        }
                except Exception as e:
                    print(f"处理参数组合 k={k}, timing_interval={timing_interval} 时发生异常: {e}")
                    all_results.append({
                        'k': k,
                        'timing_interval': timing_interval,
                        'final_return_pct': np.nan,
                        'trade_count': 0,
                        'max_drawdown': np.nan,
                        'prem_edge': np.nan,
                        'error': str(e)
                    })
    else:
        # 依次测试所有参数组合
        current_combination = 0
        for k, timing_interval in param_combinations:
            current_combination += 1
            
            # 运行单个文件的回测
            result = process_single_file(
                file_path=file_path,
                strategy=strategy,
                k=k,
                plot_trades=False,  # 先不绘图，最后只对最优参数绘图
                timing_interval=timing_interval,
                date=None
            )
            
            # 检查是否有错误
            if 'error' in result:
                print(f"  错误: {result['error']}")
                all_results.append({
                    'k': k,
                    'timing_interval': timing_interval,
                    'final_return_pct': np.nan,
                    'trade_count': 0,
                    'max_drawdown': np.nan,
                    'prem_edge': np.nan,
                    'error': result['error']
                })
                continue
            
            # 获取收益率
            return_pct = result.get('final_return_pct', np.nan)
            trade_count = result.get('trade_count', 0)
            max_drawdown = result.get('max_drawdown', np.nan)
            prem_edge = result.get('prem_edge', np.nan)
            
            # 保存结果
            param_result = {
                'k': k,
                'timing_interval': timing_interval,
                'final_return_pct': return_pct,
                'trade_count': trade_count,
                'max_drawdown': max_drawdown,
                'prem_edge': prem_edge,
                'total_profit': result.get('total_profit', np.nan),
                'final_inventory': result.get('final_inventory', np.nan),
                'avg_inventory': result.get('avg_inventory', np.nan),
                'inventory_volatility': result.get('inventory_volatility', np.nan),
                'data_points': result.get('data_points', 0)
            }
            all_results.append(param_result)
            
            # 更新最优结果（基于收益率）
            if not np.isnan(return_pct) and return_pct > best_return:
                best_return = return_pct
                best_result = {
                    'result': result,
                    'k': k,
                    'timing_interval': timing_interval,
                    'param_result': param_result
                }
    
    # 打印最优参数
    if best_result:

        print(f"{file_path} 最优参数组合: k={best_result['k']} timing_interval={best_result['timing_interval']} 收益率={best_result['param_result']['final_return_pct']:.2f}% 交易次数={best_result['param_result']['trade_count']} 最大回撤: {best_result['param_result']['max_drawdown']:.2f}")
        
        # 对最优参数绘制交易图
        if plot_trades and best_result['result'].get('trade_records'):
            file_name = os.path.basename(file_path).replace('.csv', '')
            plot_dir = os.path.join(SCRIPT_DIR, f"trade_plots_{strategy}_optimal")
            os.makedirs(plot_dir, exist_ok=True)
            plot_path = os.path.join(plot_dir, f"{file_name}_optimal_k{best_result['k']}_timing{best_result['timing_interval']}.png")
            
            plot_trades_with_premium(
                best_result['result']['df'],
                best_result['result']['trade_records'],
                file_name,
                strategy,
                save_path=plot_path
            )
    else:
        print("未找到有效的最优参数组合")
    
    # 保存结果到CSV
    if save_results:
        results_df = pd.DataFrame(all_results)
        file_name = os.path.basename(file_path).replace('.csv', '')
        save_path = f'single_stock_search_{file_name}.csv'
        results_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        # 保存最优参数摘要
        if best_result:
            summary = {
                'file_name': file_name,
                'optimal_k': best_result['k'],
                'optimal_timing_interval': best_result['timing_interval'],
                'optimal_return_pct': best_result['param_result']['final_return_pct'],
                'optimal_trade_count': best_result['param_result']['trade_count'],
                'optimal_max_drawdown': best_result['param_result']['max_drawdown'],
                'optimal_prem_edge': best_result['param_result']['prem_edge'],
                'total_combinations_tested': total_combinations
            }
            summary_df = pd.DataFrame([summary])
            base_path = r'marketmaking/premium/single_stock_search'
            if not os.path.exists(base_path):
                os.makedirs(base_path)
            summary_path = os.path.join(base_path, f'single_stock_optimal_{file_name}.csv')
            summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
    
    return {
        'all_results': all_results,
        'best_result': best_result,
        'results_df': pd.DataFrame(all_results) if all_results else pd.DataFrame()
    }


def process_single_parameter_combination(args):
    """
    包装函数，用于并行处理单个参数组合的回测
    需要作为模块级函数以便pickle序列化
    
    Args:
        args: 元组 (file_path, k, timing_interval, strategy)
    
    Returns:
        回测结果字典
    """
    file_path, k, timing_interval, strategy = args
    try:
        result = process_single_file(
            file_path=file_path,
            strategy=strategy,
            k=k,
            plot_trades=False,
            timing_interval=timing_interval,
            date=None
        )
        return {
            'k': k,
            'timing_interval': timing_interval,
            'result': result
        }
    except Exception as e:
        return {
            'k': k,
            'timing_interval': timing_interval,
            'result': {'error': str(e)}
        }


def process_single_file_wrapper(args):
    """
    包装函数，用于并行处理单个股票的参数搜索
    需要作为模块级函数以便pickle序列化
    
    Args:
        args: 元组 (file_path, k_list, timing_interval_list, strategy, plot_trades, save_results)
    
    Returns:
        搜索结果摘要字典
    """
    file_path, k_list, timing_interval_list, strategy, plot_trades, save_results = args
    try:
        search_result = single_stock_parameter_search(
            file_path=file_path,
            k_list=k_list,
            timing_interval_list=timing_interval_list,
            strategy=strategy,
            plot_trades=plot_trades,
            save_results=save_results,
            parallel=False  # 这个函数用于旧版本的并行，现在不再使用
        )
        
        # 收集最优参数摘要
        if search_result['best_result']:
            file_name = os.path.basename(file_path).replace('.csv', '')
            summary = {
                'file_name': file_name,
                'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
                'optimal_k': search_result['best_result']['k'],
                'optimal_timing_interval': search_result['best_result']['timing_interval'],
                'optimal_return_pct': search_result['best_result']['param_result']['final_return_pct'],
                'optimal_trade_count': search_result['best_result']['param_result']['trade_count'],
                'optimal_max_drawdown': search_result['best_result']['param_result']['max_drawdown'],
                'optimal_prem_edge': search_result['best_result']['param_result']['prem_edge']
            }
            return summary
        else:
            file_name = os.path.basename(file_path).replace('.csv', '')
            return {
                'file_name': file_name,
                'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
                'error': '未找到有效的最优参数组合'
            }
    except Exception as e:
        file_name = os.path.basename(file_path).replace('.csv', '')
        return {
            'file_name': file_name,
            'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
            'error': str(e)
        }


def batch_single_stock_search(
    data_dir, k_list, timing_interval_list,
    strategy='jump', max_workers=None, plot_trades=False, save_results=True, 
    parallel_params=True, name_range=None, name_path=None
):
    """
    批量对多个股票进行参数搜索（每个股票独立找到自己的最优参数）
    每个股票内部并行处理参数组合，股票之间串行处理
    
    Args:
        data_dir: 数据目录路径
        k_list: k参数列表
        timing_interval_list: timing_interval参数列表
        strategy: 策略类型
        max_workers: 最大工作进程数（用于并行处理参数组合）
        plot_trades: 是否绘制交易图
        save_results: 是否保存结果
        parallel_params: 是否并行处理参数组合（True=并行处理参数组合，False=依次处理）
        name_range: 名单范围，元组 (start, end)，1-based索引，用于截取name_path中的cb_code子集
                    如果为None，则处理所有cb_code
        name_path: 包含cb_code列的Excel路径，用于筛选需要回测的标的
        
    Returns:
        所有股票的搜索结果汇总，每个股票都有自己的最优参数
    """
    if not name_path:
        print("错误: 未提供name_path，无法获取cb_code列表")
        return []
    try:
        cb_list = pd.read_excel(name_path)['cb_code'].dropna().astype(str).tolist()
    except Exception as e:
        print(f"读取name_path失败: {e}")
        return []
    
    # 按范围截取cb_code
    if name_range is not None:
        start_idx, end_idx = name_range
        start_idx = max(0, start_idx - 1)  # 1-based转0-based
        end_idx = min(len(cb_list), end_idx)
        if start_idx >= end_idx:
            print(f"错误: 名单范围无效，start={start_idx+1}, end={end_idx}, 总数量={len(cb_list)}")
            return []
        cb_list = cb_list[start_idx:end_idx]
        print(f"将处理名单第 {start_idx+1} 到第 {end_idx} 条cb_code，共 {len(cb_list)} 个")
    else:
        print(f"将处理名单中全部 cb_code，共 {len(cb_list)} 个")
    cb_list = [i.split('.')[0] for i in cb_list]
    print(cb_list)
    # 获取数据目录下的所有CSV文件
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    print(csv_files)
    if not csv_files:
        print(f"在目录 {data_dir} 中未找到CSV文件")
        return []
    selected_files =[i for i in csv_files if any(cb_code in i for cb_code in cb_list)]
    
    total_files = len(selected_files)
    print(f"匹配到 {total_files} 个CSV文件，将逐个处理")
    
    if parallel_params:
        print(f"将依次处理股票，每个股票内部并行处理参数组合")
    else:
        print(f"将依次处理股票和参数组合")
    
    all_summaries = []
    
    args_list = []
    for i, file_path in enumerate(selected_files, 1):
        args_list.append((file_path,k_list,timing_interval_list,strategy,plot_trades,save_results,False,max_workers))
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_param = {
            executor.submit(single_stock_parameter_search_args, args): (args[0])
            for args in args_list
        }
        
        # 收集结果
        completed = 0
        for future in as_completed(future_to_param):
            try:
                completed += 1
                file_path = future_to_param[future]
                search_result = future.result()
            
                # 收集最优参数摘要
                if search_result['best_result']:
                    file_name = os.path.basename(file_path).replace('.csv', '')
                    summary = {
                        'file_name': file_name,
                        'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
                        'optimal_k': search_result['best_result']['k'],
                        'optimal_timing_interval': search_result['best_result']['timing_interval'],
                        'optimal_return_pct': search_result['best_result']['param_result']['final_return_pct'],
                        'optimal_trade_count': search_result['best_result']['param_result']['trade_count'],
                        'optimal_max_drawdown': search_result['best_result']['param_result']['max_drawdown'],
                        'optimal_prem_edge': search_result['best_result']['param_result']['prem_edge']
                    }
                    all_summaries.append(summary)
                else:
                    file_name = os.path.basename(file_path).replace('.csv', '')
                    all_summaries.append({
                        'file_name': file_name,
                        'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
                        'error': '未找到有效的最优参数组合'
                    })
            except Exception as e:
                print(f"处理文件 {file_path} 时发生异常: {e}")
                file_name = os.path.basename(file_path).replace('.csv', '')
                all_summaries.append({
                    'file_name': file_name,
                    'cb_code': file_name.split('_')[1] if '_' in file_name else file_name,
                    'error': str(e)
                })
    
    # 保存所有股票的汇总结果
    if all_summaries and save_results:
        summary_df = pd.DataFrame(all_summaries)
        summary_path = 'batch_single_stock_optimal_summary.csv'
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        print(f"\n所有股票的最优参数汇总已保存到: {summary_path}")
        
        # 打印统计信息
        valid_summaries = [s for s in all_summaries if 'error' not in s]
        if valid_summaries:
            returns = [s['optimal_return_pct'] for s in valid_summaries if 'optimal_return_pct' in s]
            if returns:
                print(f"\n汇总统计:")
                print(f"  成功处理的股票数: {len(valid_summaries)}/{len(all_summaries)}")
                print(f"  平均最优收益率: {np.mean(returns):.2f}%")
                print(f"  中位数最优收益率: {np.median(returns):.2f}%")
                print(f"  最高收益率: {np.max(returns):.2f}%")
                print(f"  最低收益率: {np.min(returns):.2f}%")
                
                # 打印参数分布统计
                if valid_summaries:
                    k_values = [s['optimal_k'] for s in valid_summaries if 'optimal_k' in s]
                    timing_values = [s['optimal_timing_interval'] for s in valid_summaries if 'optimal_timing_interval' in s]
                    if k_values:
                        print(f"\n最优参数分布:")
                        print(f"  k参数 - 最常用: {max(set(k_values), key=k_values.count)}, 范围: [{min(k_values)}, {max(k_values)}]")
                    if timing_values:
                        print(f"  timing_interval参数 - 最常用: {max(set(timing_values), key=timing_values.count)}, 范围: [{min(timing_values)}, {max(timing_values)}]")
    
    return all_summaries


def grid_search_N_rwthresh_timing_edge(
    data_dir,k_list, timing_interval_list, 
    strategy='jump', max_workers=None,date=None, plot_pnl=True
):
    results_grid = []
    pnl_plots_dir = os.path.join(SCRIPT_DIR, 'equal_weight_pnl_plots')
    os.makedirs(pnl_plots_dir, exist_ok=True)
    
    for k in k_list:
        for timing_interval in timing_interval_list:        
            print(f"\n正在测试, k={k}, timing_interval={timing_interval} ...")
            # 运行回测
            results = run_parallel_backtest(
                data_dir=data_dir,
                strategies=[strategy],
                max_workers=max_workers,
                plot_trades=False,  # 关闭单个股债的交易图以加快速度
                k=k,
                timing_interval=timing_interval,
                date=date
            )
            # 统计平均收益
            valid_results = [r for r in results if 'final_return_pct' in r]
            avg_return = np.mean([r['final_return_pct'] for r in valid_results]) if valid_results else np.nan
            print(f"k={k},timing_interval={timing_interval}, 平均收益率: {avg_return:.2f}%")
            
            # 计算等权平均pnl曲线
            if plot_pnl:
                pnl_series, dint_series = calculate_equal_weight_pnl(results)
                if pnl_series is not None and len(pnl_series) > 0:
                    plot_path = os.path.join(pnl_plots_dir, f'equal_weight_pnl_k{k}_timing{timing_interval}.png')
                    plot_equal_weight_pnl(pnl_series, dint_series, k, timing_interval, save_path=plot_path)
            
            results_grid.append({
                'k': k,
                'timing_interval': timing_interval,
                'avg_return': avg_return
            })
    return pd.DataFrame(results_grid)

def main():
    # ========== 并行批量搜索单个股票最优参数 ==========
    # 设置参数搜索范围
    k_list = [x / 10 for x in range(10, 39, 2)]  # k参数列表
    timing_interval_list = [600, 900, 1200, 2400, 3600, 4800]  # timing_interval参数列表
    data_directory = cfg.prem_dir + '/' + cfg.start_date + '/'
    
    print("="*80)
    print("开始并行批量搜索单个股票最优参数")
    print("="*80)
    print(f"参数搜索范围:")
    print(f"  k: {k_list}")
    print(f"  timing_interval: {timing_interval_list}")
    print(f"  总组合数: {len(k_list) * len(timing_interval_list)} 个参数组合/股票")
    print(f"数据目录: {data_directory}")
    print("="*80)
    
    # 批量对多个股票进行参数搜索（每个股票独立找最优参数）
    # 每个股票内部并行处理参数组合，股票之间串行处理
    # name_range: 可选，指定文件范围，例如 (5, 20) 表示处理第5到第20个文件
    # 如果为 None，则处理所有文件
    batch_summaries = batch_single_stock_search(
        data_dir=data_directory,
        k_list=k_list,
        timing_interval_list=timing_interval_list,
        strategy='jump',
        plot_trades=True,  # 批量处理时不绘图以加快速度
        save_results=True,
        parallel_params=True,  # 并行处理参数组合（每个股票内部并行）
        max_workers=os.cpu_count(),  # 并行处理参数组合时的进程数
        name_range=None,  # 可选：指定处理第1到第2个文件，设置为None则处理所有文件
        name_path=f"{cfg.config_dir}/cb_filter.xlsx"
    )
    
    print("\n" + "="*80)
    print("并行搜索完成！")
    print("="*80)
    
    # 显示结果摘要
    if batch_summaries:
        summary_df = pd.DataFrame(batch_summaries)
        print("\n前10个股票的最优参数:")
        print(summary_df.head(10).to_string(index=False))
        
        # 保存详细结果
        summary_df.to_csv(cfg.result_path, index=False, encoding='utf-8-sig')
        print(f"\n所有结果已保存)")

if __name__ == '__main__':
    main()
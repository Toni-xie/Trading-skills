#!/usr/bin/env python3
"""
Binance Token Analysis - Complete Solution
整合脚本：获取数据、分析共同代币、生成报告
"""

import requests
import json
import os
from datetime import datetime

# ==========================================
# 数据获取部分
# ==========================================

def fetch_data():
    """从Binance API获取数据"""
    print("=" * 80)
    print("步骤 1: 获取数据")
    print("=" * 80)

    # 确保数据目录存在
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"创建数据目录: {data_dir}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 获取Alpha代币列表
    print("\n正在获取Alpha代币列表...")
    alpha_data = fetch_alpha_tokens()
    if alpha_data:
        alpha_filename = f"Alpha_list_{timestamp}.json"
        save_json_data(alpha_data, os.path.join(data_dir, alpha_filename))
        print(f"[OK] Alpha数据已保存: {alpha_filename}")
    else:
        print("[FAIL] Alpha数据获取失败")
        return None, None

    # 获取合约代币列表
    print("\n正在获取合约代币列表...")
    future_data = fetch_future_tokens()
    if future_data:
        future_filename = f"future_list_{timestamp}.json"
        save_json_data(future_data, os.path.join(data_dir, future_filename))
        print(f"[OK] Future数据已保存: {future_filename}")
    else:
        print("[FAIL] Future数据获取失败")
        return None, None

    return alpha_data, future_data

def fetch_alpha_tokens():
    """获取Alpha代币列表"""
    url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取Alpha数据失败: {e}")
        return None

def fetch_future_tokens():
    """获取合约代币列表"""
    url = "https://www.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取Future数据失败: {e}")
        return None

def save_json_data(data, filepath):
    """保存JSON数据"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"保存数据失败: {e}")
        return False

# ==========================================
# 数据分析部分
# ==========================================

def analyze_data(alpha_data, future_data):
    """分析Alpha和Future数据，找出共同代币"""
    print("\n" + "=" * 80)
    print("步骤 2: 数据分析")
    print("=" * 80)

    if not alpha_data or not future_data:
        print("数据不完整，无法进行分析")
        return None

    # 提取Alpha代币（过滤offline代币）
    alpha_tokens = extract_alpha_tokens(alpha_data)

    # 提取Future代币（只保留TRADING状态）
    future_symbols = extract_future_symbols(future_data)

    # 找出共同代币
    common_symbols = set(alpha_tokens.keys()) & future_symbols

    print(f"\n分析结果:")
    print(f"  Alpha在线代币: {len(alpha_tokens)}")
    print(f"  Future交易代币: {len(future_symbols)}")
    print(f"  共同代币: {len(common_symbols)}")

    if not common_symbols:
        print("没有找到共同代币")
        return None

    # 获取共同代币的详细信息
    common_tokens = []
    for symbol in sorted(common_symbols):
        if symbol in alpha_tokens:
            common_tokens.append(alpha_tokens[symbol])

    # 按市值从小到大排序
    common_tokens_sorted = sorted(common_tokens, key=lambda x: x['market_cap'])

    return common_tokens_sorted

def extract_alpha_tokens(alpha_data):
    """提取Alpha代币信息，过滤offline代币"""
    if not alpha_data or "data" not in alpha_data:
        return {}

    tokens = {}
    offline_count = 0

    for token in alpha_data["data"]:
        # 过滤offline代币
        if token.get("offline", False) == True or token.get("offline") == "true":
            offline_count += 1
            continue

        if "symbol" in token:
            tokens[token["symbol"]] = {
                "symbol": token.get("symbol", "N/A"),
                "name": token.get("name", "N/A"),
                "price": safe_float(token.get("price", 0)),
                "percent_change_24h": safe_float(token.get("percentChange24h", 0)),
                "volume_24h": safe_float(token.get("volume24h", 0)),
                "market_cap": safe_float(token.get("marketCap", 0)),
                "chain": token.get("chainName", "N/A"),
                "contract_address": token.get("contractAddress", "N/A")
            }

    print(f"  过滤掉{offline_count}个offline代币，剩余{len(tokens)}个在线代币")
    return tokens

def extract_future_symbols(future_data):
    """提取Future代币符号，只保留TRADING状态"""
    if not future_data or "symbols" not in future_data:
        return set()

    base_assets = set()
    total_count = 0
    trading_count = 0
    filtered_count = 0

    for symbol_info in future_data["symbols"]:
        if "symbol" in symbol_info:
            total_count += 1
            symbol = symbol_info["symbol"]

            # 只保留TRADING状态
            if symbol_info.get("status") != "TRADING":
                filtered_count += 1
                continue

            if symbol.endswith("USDT"):
                base_asset = symbol[:-4]  # 去掉USDT后缀
                base_assets.add(base_asset)
                trading_count += 1

    print(f"  过滤掉{filtered_count}个非TRADING代币，保留{trading_count}个交易代币")
    return base_assets

def safe_float(value, default=0.0):
    """安全转换为浮点数"""
    try:
        if value == "" or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

# ==========================================
# 数据格式化部分
# ==========================================

def format_large_number(number):
    """格式化大数字（K, M, B, T）"""
    if number == 0:
        return "$0"

    abs_number = abs(number)

    if abs_number >= 1_000_000_000_000:  # 万亿
        return f"${number / 1_000_000_000_000:.2f}T"
    elif abs_number >= 1_000_000_000:  # 十亿
        return f"${number / 1_000_000_000:.2f}B"
    elif abs_number >= 1_000_000:  # 百万
        return f"${number / 1_000_000:.2f}M"
    elif abs_number >= 1_000:  # 千
        return f"${number / 1_000:.2f}K"
    else:
        return f"${number:.2f}"

# ==========================================
# 报告生成部分
# ==========================================

def generate_report(common_tokens):
    """生成分析报告"""
    print("\n" + "=" * 80)
    print("步骤 3: 生成报告")
    print("=" * 80)

    if not common_tokens:
        print("没有数据可以生成报告")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 生成JSON报告
    json_filename = f"analysis_result_{timestamp}.json"
    json_filepath = os.path.join("data", json_filename)
    save_json_report(common_tokens, json_filepath)
    print(f"[OK] JSON报告已保存: {json_filename}")

    # 生成CSV报告
    csv_filename = f"analysis_result_{timestamp}.csv"
    csv_filepath = os.path.join("data", csv_filename)
    save_csv_report(common_tokens, csv_filepath)
    print(f"[OK] CSV报告已保存: {csv_filename}")

    # 显示摘要报告
    display_summary_report(common_tokens)

def save_json_report(tokens, filepath):
    """保存JSON格式报告"""
    try:
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "total_tokens": len(tokens),
            "analysis_criteria": {
                "alpha_tokens": "online only (offline=false)",
                "future_tokens": "TRADING status only"
            },
            "tokens": tokens
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"保存JSON报告失败: {e}")

def save_csv_report(tokens, filepath):
    """保存CSV格式报告"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            # 写入标题行
            f.write("Rank,Symbol,Name,Price,24h Change,24h Volume,Market Cap,Chain,Contract Address\n")

            # 写入数据行
            for i, token in enumerate(tokens, 1):
                price = f"${token['price']:.4f}"
                change = f"{token['percent_change_24h']:.2f}%"
                volume = format_large_number(token['volume_24h']).replace('$', '')
                market_cap = format_large_number(token['market_cap']).replace('$', '')

                f.write(f"{i},{token['symbol']},{token['name']},{price},{change},{volume},{market_cap},{token['chain']},{token['contract_address']}\n")
    except Exception as e:
        print(f"保存CSV报告失败: {e}")

def display_summary_report(tokens):
    """显示摘要报告"""
    print("\n" + "=" * 100)
    print("分析摘要报告")
    print("=" * 100)

    # 显示前20个代币
    print(f"\n{'排名':<6} {'代币':<10} {'名称':<20} {'价格':<12} {'24h涨跌':<10} {'24h交易量':<12} {'市值':<12} {'网络':<8}")
    print("-" * 100)

    display_tokens = tokens[:20]  # 只显示前20个
    for i, token in enumerate(display_tokens, 1):
        symbol = token['symbol']
        name = token['name'][:19]
        price = f"${token['price']:.4f}"
        change = f"{token['percent_change_24h']:+.2f}%"
        volume = format_large_number(token['volume_24h'])
        market_cap = format_large_number(token['market_cap'])
        chain = token['chain']

        print(f"{i:<6} {symbol:<10} {name:<20} {price:<12} {change:<10} {volume:<12} {market_cap:<12} {chain:<8}")

    print(f"\n... (共{len(tokens)}个代币，完整数据见CSV文件)")

    # 显示统计信息
    print("\n" + "=" * 100)
    print("统计信息")
    print("=" * 100)

    # 市值分布
    micro_cap = sum(1 for t in tokens if t['market_cap'] < 10_000_000)
    small_cap = sum(1 for t in tokens if 10_000_000 <= t['market_cap'] < 50_000_000)
    mid_cap = sum(1 for t in tokens if 50_000_000 <= t['market_cap'] < 200_000_000)
    large_cap = sum(1 for t in tokens if 200_000_000 <= t['market_cap'] < 1_000_000_000)
    mega_cap = sum(1 for t in tokens if t['market_cap'] >= 1_000_000_000)

    print(f"市值分布:")
    print(f"  Micro Cap (<$10M): {micro_cap}个 ({micro_cap/len(tokens)*100:.1f}%)")
    print(f"  Small Cap ($10M-$50M): {small_cap}个 ({small_cap/len(tokens)*100:.1f}%)")
    print(f"  Mid Cap ($50M-$200M): {mid_cap}个 ({mid_cap/len(tokens)*100:.1f}%)")
    print(f"  Large Cap ($200M-$1B): {large_cap}个 ({large_cap/len(tokens)*100:.1f}%)")
    print(f"  Mega Cap (>$1B): {mega_cap}个 ({mega_cap/len(tokens)*100:.1f}%)")

    # 涨跌幅统计
    gainers = [t for t in tokens if t['percent_change_24h'] > 0]
    losers = [t for t in tokens if t['percent_change_24h'] < 0]

    print(f"\n涨跌统计:")
    print(f"  上涨代币: {len(gainers)}个 ({len(gainers)/len(tokens)*100:.1f}%)")
    print(f"  下跌代币: {len(losers)}个 ({len(losers)/len(tokens)*100:.1f}%)")

    if tokens:
        print(f"  平均涨跌: {sum(t['percent_change_24h'] for t in tokens)/len(tokens):+.2f}%")

    # TOP5
    print(f"\n市值TOP5:")
    for i, token in enumerate(reversed(tokens[-5:]), 1):
        print(f"  {i}. {token['symbol']:<8} - {format_large_number(token['market_cap'])}")

    print(f"\n涨幅TOP5:")
    top_gainers = sorted(tokens, key=lambda x: x['percent_change_24h'], reverse=True)[:5]
    for i, token in enumerate(top_gainers, 1):
        print(f"  {i}. {token['symbol']:<8} - {token['percent_change_24h']:+.2f}%")

    print(f"\n跌幅TOP5:")
    top_losers = sorted(tokens, key=lambda x: x['percent_change_24h'])[:5]
    for i, token in enumerate(top_losers, 1):
        print(f"  {i}. {token['symbol']:<8} - {token['percent_change_24h']:+.2f}%")

    print("\n" + "=" * 100)

# ==========================================
# 主程序
# ==========================================

def main():
    """主程序"""
    print("=" * 80)
    print("Binance代币分析工具 - 完整解决方案")
    print("=" * 80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 步骤1: 获取数据
        alpha_data, future_data = fetch_data()

        if not alpha_data or not future_data:
            print("\n数据获取失败，程序退出")
            return

        # 步骤2: 分析数据
        common_tokens = analyze_data(alpha_data, future_data)

        if not common_tokens:
            print("\n分析失败，程序退出")
            return

        # 步骤3: 生成报告
        generate_report(common_tokens)

        print("\n" + "=" * 80)
        print("[SUCCESS] 分析完成！")
        print("=" * 80)
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"分析代币数量: {len(common_tokens)}")
        print(f"报告保存位置: data/ 目录")
        print("\n你可以直接打开CSV文件查看完整结果")

    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
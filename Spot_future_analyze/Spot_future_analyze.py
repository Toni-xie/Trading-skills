#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
一键拉取：
1) 同时在币安现货和币安U本位合约上线的代币（默认按 USDT 交易对匹配）
2) 聚合多个公开接口返回的全部指标字段
3) 导出 CSV

依赖：
    pip install requests pandas

运行：
    python binance_dual_listed.py

输出：
    dual_listed_spot_futures_metrics.csv
"""

from __future__ import annotations

import time
import os
from pathlib import Path
from typing import Dict, List, Any, Iterable, Optional
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 dual-listed-binance-script/1.0"
})

TIMEOUT = 20

# 列名中文翻译字典
COLUMN_TRANSLATIONS = {
    "symbol": "交易对",
    "base_asset": "基础资产",
    "quote_asset": "计价资产",
    "listed_in_spot": "现货已上线",
    "listed_in_usdm_perp": "U本位合约已上线",
    
    # 现货 exchangeInfo
    "spot_info_status": "现货_状态",
    "spot_info_baseAsset": "现货_基础资产",
    "spot_info_quoteAsset": "现货_计价资产",
    "spot_info_pricePrecision": "现货_价格精度",
    "spot_info_quantityPrecision": "现货_数量精度",
    "spot_info_baseAssetPrecision": "现货_基础资产精度",
    "spot_info_quotePrecision": "现货_计价资产精度",
    
    # 合约 exchangeInfo
    "futures_info_status": "合约_状态",
    "futures_info_baseAsset": "合约_基础资产",
    "futures_info_quoteAsset": "合约_计价资产",
    "futures_info_contractType": "合约_类型",
    "futures_info_pricePrecision": "合约_价格精度",
    "futures_info_quantityPrecision": "合约_数量精度",
    
    # 现货 24hr ticker
    "spot_24hr_symbol": "现货24h_交易对",
    "spot_24hr_priceChange": "现货24h_价格变化",
    "spot_24hr_priceChangePercent": "现货24h_价格变化百分比",
    "spot_24hr_weightedAvgPrice": "现货24h_加权平均价",
    "spot_24hr_lastPrice": "现货24h_最新价格",
    "spot_24hr_lastQty": "现货24h_最新成交量",
    "spot_24hr_openPrice": "现货24h_开盘价",
    "spot_24hr_highPrice": "现货24h_最高价",
    "spot_24hr_lowPrice": "现货24h_最低价",
    "spot_24hr_volume": "现货24h_成交量",
    "spot_24hr_quoteAssetVolume": "现货24h_成交额",
    "spot_24hr_openTime": "现货24h_开盘时间",
    "spot_24hr_closeTime": "现货24h_收盘时间",
    
    # 现货 Book Ticker
    "spot_book_symbol": "现货挂单_交易对",
    "spot_book_bidPrice": "现货挂单_买价",
    "spot_book_bidQty": "现货挂单_买量",
    "spot_book_askPrice": "现货挂单_卖价",
    "spot_book_askQty": "现货挂单_卖量",
    
    # 合约 24hr ticker
    "futures_24hr_symbol": "合约24h_交易对",
    "futures_24hr_priceChange": "合约24h_价格变化",
    "futures_24hr_priceChangePercent": "合约24h_价格变化百分比",
    "futures_24hr_weightedAvgPrice": "合约24h_加权平均价",
    "futures_24hr_lastPrice": "合约24h_最新价格",
    "futures_24hr_lastQty": "合约24h_最新成交量",
    "futures_24hr_openPrice": "合约24h_开盘价",
    "futures_24hr_highPrice": "合约24h_最高价",
    "futures_24hr_lowPrice": "合约24h_最低价",
    "futures_24hr_volume": "合约24h_成交量",
    "futures_24hr_quoteAssetVolume": "合约24h_成交额",
    
    # 合约 Book Ticker
    "futures_book_symbol": "合约挂单_交易对",
    "futures_book_bidPrice": "合约挂单_买价",
    "futures_book_bidQty": "合约挂单_买量",
    "futures_book_askPrice": "合约挂单_卖价",
    "futures_book_askQty": "合约挂单_卖量",
    
    # 合约 Mark Price & Funding
    "futures_mark_symbol": "合约标记_交易对",
    "futures_mark_markPrice": "合约标记_标记价格",
    "futures_mark_indexPrice": "合约标记_指数价格",
    "futures_mark_estimatedSettlePrice": "合约标记_预计结算价",
    "futures_mark_lastFundingRate": "合约标记_最后资金费率",
    "futures_mark_interestRate": "合约标记_利率",
    "futures_mark_nextFundingTime": "合约标记_下次资金费时间",
    "futures_mark_time": "合约标记_时间",
    
    # 合约 Open Interest
    "futures_oi_symbol": "合约持仓_交易对",
    "futures_oi_openInterest": "合约持仓_持仓量",
    "futures_oi_time": "合约持仓_时间",
}


def translate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """将DataFrame的列名翻译成中文。"""
    new_columns = {}
    for col in df.columns:
        new_columns[col] = COLUMN_TRANSLATIONS.get(col, col)
    return df.rename(columns=new_columns)


def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET 请求并返回 JSON。"""
    resp = SESSION.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    """按 size 切块。"""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def safe_float(x: Any) -> Any:
    """尽量把字符串数字转成 float，失败则原样返回。"""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        try:
            if x.strip() == "":
                return x
            return float(x)
        except ValueError:
            return x
    return x


def flatten_dict(d: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """给字典字段统一加前缀。"""
    out = {}
    for k, v in d.items():
        out[f"{prefix}{k}"] = safe_float(v)
    return out


def get_spot_symbols() -> List[Dict[str, Any]]:
    """
    获取现货交易对。
    使用 exchangeInfo。
    """
    data = get_json(f"{SPOT_BASE}/api/v3/exchangeInfo")
    return data.get("symbols", [])


def get_futures_symbols() -> List[Dict[str, Any]]:
    """
    获取 U 本位合约交易对。
    使用 exchangeInfo。
    """
    data = get_json(f"{FUTURES_BASE}/fapi/v1/exchangeInfo")
    return data.get("symbols", [])


def filter_spot_usdt_symbols(symbols: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    过滤出可交易的现货 USDT 交易对。
    """
    result = {}
    for s in symbols:
        symbol = s.get("symbol")
        quote = s.get("quoteAsset")
        status = s.get("status")
        permissions = s.get("permissions", [])

        # 现货文档里 permissions / permissionSets 机制有过更新，
        # 这里保守判断：quoteAsset=USDT 且状态为 TRADING 即可。
        if quote == "USDT" and status == "TRADING":
            result[symbol] = s
    return result


def filter_futures_usdt_perps(symbols: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    过滤出可交易的 U 本位 USDT 永续合约。
    """
    result = {}
    for s in symbols:
        symbol = s.get("symbol")
        quote = s.get("quoteAsset")
        status = s.get("status")
        contract_type = s.get("contractType")

        if quote == "USDT" and status == "TRADING" and contract_type == "PERPETUAL":
            result[symbol] = s
    return result


def get_spot_24hr_all() -> Dict[str, Dict[str, Any]]:
    """
    获取现货所有交易对的 24hr ticker。
    文档支持不传 symbol 时返回全部。
    """
    data = get_json(f"{SPOT_BASE}/api/v3/ticker/24hr")
    return {item["symbol"]: item for item in data}


def get_spot_book_ticker_all() -> Dict[str, Dict[str, Any]]:
    """
    获取现货所有交易对的 best bid/ask。
    """
    data = get_json(f"{SPOT_BASE}/api/v3/ticker/bookTicker")
    return {item["symbol"]: item for item in data}


def get_futures_24hr_all() -> Dict[str, Dict[str, Any]]:
    """
    获取 U 本位合约所有交易对的 24hr ticker。
    """
    data = get_json(f"{FUTURES_BASE}/fapi/v1/ticker/24hr")
    return {item["symbol"]: item for item in data}


def get_futures_book_ticker_all() -> Dict[str, Dict[str, Any]]:
    """
    获取 U 本位合约所有交易对的 best bid/ask。
    """
    data = get_json(f"{FUTURES_BASE}/fapi/v1/ticker/bookTicker")
    return {item["symbol"]: item for item in data}


def get_futures_mark_price_all() -> Dict[str, Dict[str, Any]]:
    """
    获取 U 本位合约所有交易对的 mark price / funding 相关数据。
    premiumIndex 接口不传 symbol 时返回全部。
    """
    data = get_json(f"{FUTURES_BASE}/fapi/v1/premiumIndex")
    if isinstance(data, dict):
        data = [data]
    return {item["symbol"]: item for item in data}


def get_futures_open_interest(symbols: List[str], sleep_sec: float = 0.03, max_workers: int = 5) -> Dict[str, Dict[str, Any]]:
    """
    openInterest 接口必须逐个 symbol 请求。
    使用线程池加速请求。
    """
    result = {}
    
    def fetch_oi(symbol: str) -> tuple:
        """获取单个symbol的openInterest数据。"""
        try:
            data = get_json(f"{FUTURES_BASE}/fapi/v1/openInterest", params={"symbol": symbol})
            time.sleep(sleep_sec)
            return symbol, data
        except Exception as e:
            return symbol, {"symbol": symbol, "openInterest_error": str(e)}
    
    total = len(symbols)
    completed = 0
    
    print(f"  获取 {total} 个交易对的持仓量数据 (使用 {max_workers} 个并行线程)...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_oi, symbol): symbol for symbol in symbols}
        
        for future in as_completed(futures):
            symbol, data = future.result()
            result[symbol] = data
            completed += 1
            
            # 显示进度
            if completed % max(1, total // 10) == 0 or completed == total:
                print(f"    进度: {completed}/{total} ({completed*100//total}%)")
    
    return result


def extract_exchange_meta(raw_symbol_info: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """
    从 exchangeInfo 的 symbol info 提取一些基础字段。
    同时保留 filters 原始内容，便于后续扩展。
    """
    keep_keys = [
        "symbol", "status", "baseAsset", "quoteAsset", "contractType",
        "pricePrecision", "quantityPrecision", "baseAssetPrecision",
        "quotePrecision", "underlyingType", "underlyingSubType",
        "triggerProtect", "liquidationFee", "marketTakeBound"
    ]
    out = {}
    for k in keep_keys:
        if k in raw_symbol_info:
            out[f"{prefix}{k}"] = raw_symbol_info[k]

    # filters 保留为字符串，方便你后面自己解析 tickSize / stepSize / minQty / minNotional
    if "filters" in raw_symbol_info:
        out[f"{prefix}filters"] = str(raw_symbol_info["filters"])

    # 兼容 permissions / permissionSets
    if "permissions" in raw_symbol_info:
        out[f"{prefix}permissions"] = str(raw_symbol_info["permissions"])
    if "permissionSets" in raw_symbol_info:
        out[f"{prefix}permissionSets"] = str(raw_symbol_info["permissionSets"])

    return out


def main() -> None:
    print("Step 1/6: 拉取现货 exchangeInfo ...")
    spot_symbols_raw = get_spot_symbols()
    spot_symbols = filter_spot_usdt_symbols(spot_symbols_raw)
    print(f"现货可交易 USDT 交易对数: {len(spot_symbols)}")

    print("Step 2/6: 拉取 U 本位合约 exchangeInfo ...")
    futures_symbols_raw = get_futures_symbols()
    futures_symbols = filter_futures_usdt_perps(futures_symbols_raw)
    print(f"U本位永续可交易 USDT 合约数: {len(futures_symbols)}")

    common_symbols = sorted(set(spot_symbols.keys()) & set(futures_symbols.keys()))
    print(f"同时在现货和U本位永续上线的 USDT 交易对数: {len(common_symbols)}")

    print("Step 3/6: 拉取现货 ticker / bookTicker ...")
    spot_24hr = get_spot_24hr_all()
    print(f"  获取 {len(spot_24hr)} 个现货 24hr ticker")
    spot_book = get_spot_book_ticker_all()
    print(f"  获取 {len(spot_book)} 个现货 bookTicker")

    print("Step 4/6: 拉取合约 ticker / bookTicker / premiumIndex ...")
    fut_24hr = get_futures_24hr_all()
    print(f"  获取 {len(fut_24hr)} 个合约 24hr ticker")
    fut_book = get_futures_book_ticker_all()
    print(f"  获取 {len(fut_book)} 个合约 bookTicker")
    fut_mark = get_futures_mark_price_all()
    print(f"  获取 {len(fut_mark)} 个合约 mark price")

    print("Step 5/6: 逐个拉取 openInterest ...")
    # 增加 skip_oi 参数，设为 True 可跳过持仓量数据获取以加快速度
    skip_oi = False  # 改为 True 可跳过
    if skip_oi:
        print("  (已跳过)")
        fut_oi = {}
    else:
        fut_oi = get_futures_open_interest(common_symbols, max_workers=5)

    print("Step 6/6: 聚合并导出 CSV ...")
    rows: List[Dict[str, Any]] = []

    for symbol in common_symbols:
        row: Dict[str, Any] = {
            "symbol": symbol,
            "base_asset": spot_symbols[symbol].get("baseAsset"),
            "quote_asset": "USDT",
            "listed_in_spot": True,
            "listed_in_usdm_perp": True,
        }

        row.update(extract_exchange_meta(spot_symbols[symbol], "spot_info_"))
        row.update(extract_exchange_meta(futures_symbols[symbol], "futures_info_"))

        if symbol in spot_24hr:
            row.update(flatten_dict(spot_24hr[symbol], "spot_24hr_"))

        if symbol in spot_book:
            row.update(flatten_dict(spot_book[symbol], "spot_book_"))

        if symbol in fut_24hr:
            row.update(flatten_dict(fut_24hr[symbol], "futures_24hr_"))

        if symbol in fut_book:
            row.update(flatten_dict(fut_book[symbol], "futures_book_"))

        if symbol in fut_mark:
            row.update(flatten_dict(fut_mark[symbol], "futures_mark_"))

        if symbol in fut_oi:
            row.update(flatten_dict(fut_oi[symbol], "futures_oi_"))

        rows.append(row)

    df = pd.DataFrame(rows)

    # 按 base_asset 排序更直观
    sort_cols = [c for c in ["base_asset", "symbol"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    # 创建data文件夹（如果不存在）
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # 翻译列名为中文
    df_translated = translate_columns(df)
    
    output_file = data_dir / "dual_listed_spot_futures_metrics.csv"
    df_translated.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"\n完成。已导出: {output_file}")
    print(f"总行数: {len(df)}")
    print(f"总字段数: {len(df.columns)}")

    # 顺便打印“接口字段清单”
    print("\n====== 字段来源清单 ======")
    groups = {
        "spot_info_": "现货 exchangeInfo",
        "futures_info_": "合约 exchangeInfo",
        "spot_24hr_": "现货 24hr ticker",
        "spot_book_": "现货 bookTicker",
        "futures_24hr_": "合约 24hr ticker",
        "futures_book_": "合约 bookTicker",
        "futures_mark_": "合约 premiumIndex / mark price",
        "futures_oi_": "合约 openInterest",
    }
    for prefix, name in groups.items():
        cols = [c for c in df.columns if c.startswith(prefix)]
        print(f"\n{name} ({len(cols)} fields):")
        for c in cols:
            print("  -", c)


if __name__ == "__main__":
    main()
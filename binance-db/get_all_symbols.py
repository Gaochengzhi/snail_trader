import requests
import json


def get_usdtm_symbols():
    """获取所有USDT-M交易对"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    data = requests.get(url, timeout=5).json()
    return [s["symbol"] for s in data["symbols"]]


def export_symbols_to_json(filename="symbols.json"):
    """将所有交易对导出到JSON文件"""
    symbols = get_usdtm_symbols()

    # 创建JSON格式的内容
    json_data = {"symbols": symbols}

    # 写入文件
    with open(filename, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"成功导出 {len(symbols)} 个交易对到 {filename}")


if __name__ == "__main__":
    export_symbols_to_json()


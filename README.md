# 新增山寨赛道龙头分析脚本
在 data文件夹24h_change_table.xlsx中添加想要抓取信息的代币
脚本自动化计算过去24小时和7天价格和OI变化幅度
运行extract_24h_change.py即可

# Binance代币分析Skills - 使用说明
## 个人安装（所有项目可用）
unzip binance-token-analysis.skill -d ~/.claude/skills/

## 或者项目级安装（只在当前项目生效）
unzip binance-token-analysis.skill -d .claude/skills/

# Binance代币分析脚本 - 使用说明

## 🚀 快速开始

只需运行一个命令：

```bash
python Future_Alpha_analysis.py
```

## 📋 功能说明

这个整合脚本会自动完成以下步骤：

### 步骤1: 获取数据
- 从Binance API获取Alpha代币列表
- 从Binance API获取合约代币列表
- 自动保存原始数据到 `data/` 目录

### 步骤2: 数据分析
- 过滤offline状态的Alpha代币
- 只保留TRADING状态的合约代币
- 找出两个列表中的共同代币
- 按市值从小到大排序

### 步骤3: 生成报告
- 生成JSON格式报告（包含完整数据）
- 生成CSV格式报告（可用Excel打开）
- 显示分析摘要和统计信息

## 📊 输出文件

运行完成后，在 `data/` 目录下会生成：

1. **Alpha_list_YYYYMMDD_HHMMSS.json** - Alpha代币原始数据
2. **future_list_YYYYMMDD_HHMMSS.json** - 合约代币原始数据
3. **analysis_result_YYYYMMDD_HHMMSS.json** - 完整分析结果（JSON格式）
4. **analysis_result_YYYYMMDD_HHMMSS.csv** - 分析结果（CSV格式）

## 📈 CSV文件内容

CSV文件包含以下列：
- **Rank** - 按市值排名
- **Symbol** - 代币符号
- **Name** - 代币名称
- **Price** - 当前价格（4位小数）
- **24h Change** - 24小时涨跌幅
- **24h Volume** - 24小时交易量（M/B格式）
- **Market Cap** - 市值（M/B格式）
- **Chain** - 区块链网络
- **Contract Address** - 合约地址

## 🔍 数据说明

### 过滤条件
- **Alpha代币**: 只包含在线代币（offline=false）
- **合约代币**: 只包含交易状态代币（status=TRADING）
- **排序方式**: 按市值从小到大排列

### 价格格式
- 所有价格精确到小数点后4位
- 例如: $0.0001, $0.8103, $8.9348

### 数字格式
- **K** = 千 (1,000)
- **M** = 百万 (1,000,000)
- **B** = 十亿 (1,000,000,000)
- **T** = 万亿 (1,000,000,000,000)

## 📊 统计信息示例

脚本运行时会显示：
- 市值分布（Micro/Small/Mid/Large/Mega Cap）
- 涨跌统计
- 市值TOP5
- 涨幅TOP5
- 跌幅TOP5

## 💡 使用建议

1. **定期运行**: 市场数据实时变化，建议定期运行获取最新数据
2. **CSV分析**: 生成的CSV文件可直接在Excel中打开进行进一步分析
3. **数据备份**: 原始JSON文件可用于历史数据对比

## ⚠️ 注意事项

- 需要稳定的网络连接访问Binance API
- 如遇网络错误，请重新运行脚本
- 生成的报告文件会带时间戳，不会覆盖之前的文件

## 🎯 一键运行

最简单的使用方式：

```bash
python Future_Alpha_analysis.py
```

然后打开 `data/` 目录中最新生成的CSV文件查看结果！

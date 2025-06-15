# Sakila Text2SQL 评估体系

这是一个完整的Text2SQL模型评估系统，专门用于评估在Sakila电影租赁数据库上的Text2SQL性能。

## 🎯 项目概述

本评估系统实现了完整的Text2SQL评估流程，包括：
- 数据库初始化和管理
- 多维度评估指标计算
- 有参照系统的评估方法
- 详细的评估报告生成

## 📁 文件结构

```
Text2SQL/
├── 01-Text2SQL-创建数据库表.py          # 原始旅游数据库创建脚本
├── 02-Text2SQL-LLM-DeepSeek.py         # DeepSeek模型Text2SQL实现
├── 02-Text2SQL-LLM-OpenAI.py           # OpenAI模型Text2SQL实现
├── 03-Sakila-Text2SQL-评估体系.py        # 核心评估系统
├── 04-Sakila-数据库初始化.py            # Sakila数据库初始化脚本
├── 05-运行示例.py                       # 评估系统运行示例
└── README.md                           # 本文档
```

## 🚀 快速开始

### 环境准备

1. **安装必要的Python包**
```bash
pip install openai numpy sqlite3
```

2. **设置API密钥**
```bash
# 对于DeepSeek API
export DEEPSEEK_API_KEY="your_deepseek_api_key"

# 或者对于OpenAI API
export OPENAI_API_KEY="your_openai_api_key"
```

### 运行评估

1. **快速测试**
```bash
python 05-运行示例.py
# 选择选项1进行快速测试
```

2. **完整评估**
```bash
python 03-Sakila-Text2SQL-评估体系.py
```

3. **单独初始化数据库**
```bash
python 04-Sakila-数据库初始化.py
```

## 📊 评估指标

### 核心指标

1. **精确匹配准确率 (Exact Match Accuracy)**
   - 计算生成的SQL与标准SQL完全匹配的比例
   - 范围: 0-1，越高越好
   - 公式: `完全匹配数 / 总测试样本数`

2. **执行准确度 (Execution Accuracy)**
   - 计算生成的SQL执行结果与标准SQL执行结果相同的比例
   - 范围: 0-1，越高越好
   - **这是最重要的指标**，因为它关注结果的正确性

3. **平均SQL相似度 (Average SQL Similarity)**
   - 使用字符串相似度算法计算SQL语句的相似程度
   - 范围: 0-1，越高越好
   - 基于SequenceMatcher算法

4. **SQL可执行率 (Execution Success Rate)**
   - 计算生成的SQL能够成功执行的比例
   - 范围: 0-1，越高越好
   - 反映生成SQL的语法正确性

### 为什么选择这些指标？

- **有参照系统评估**: 我们有标准答案（ground truth），可以进行定量比较
- **结果导向**: 执行准确度比语法匹配更重要，因为我们关心的是查询结果的正确性
- **多维度评估**: 从语法、语义、执行三个层面全面评估模型性能

## 🗃️ 数据库结构

### Sakila数据库

Sakila是一个经典的MySQL示例数据库，包含电影租赁业务的完整数据模型：

#### 核心表
- **actor**: 演员信息
- **film**: 电影信息
- **category**: 电影类别
- **customer**: 客户信息
- **rental**: 租赁记录
- **payment**: 支付记录
- **inventory**: 库存信息
- **staff**: 员工信息
- **store**: 商店信息

#### 关系表
- **film_actor**: 电影-演员多对多关系
- **film_category**: 电影-类别多对多关系

### 测试数据集

从`q2sql_pairs.json`中选择25个代表性样本，包含：
- 50% SELECT查询（数据检索）
- 17% INSERT语句（数据插入）
- 17% UPDATE语句（数据更新）
- 16% DELETE语句（数据删除）

## 🔧 核心组件

### SakilaText2SQLEvaluator类

主要功能：
- `select_test_subset()`: 智能选择测试数据子集
- `generate_sql()`: 使用LLM生成SQL查询
- `evaluate_single_query()`: 评估单个查询
- `evaluate_dataset()`: 批量评估数据集
- `print_evaluation_summary()`: 打印评估摘要
- `save_results()`: 保存评估结果

### 评估流程

1. **数据库初始化**: 创建Sakila数据库和示例数据
2. **测试集选择**: 从问答对中选择代表性样本
3. **SQL生成**: 使用LLM将自然语言转换为SQL
4. **多维度评估**: 计算各项评估指标
5. **结果分析**: 生成详细的评估报告

## 📈 使用案例

### 评估不同模型性能

```python
# 评估DeepSeek模型
evaluator_deepseek = SakilaText2SQLEvaluator(
    model="deepseek-chat",
    base_url="https://api.deepseek.com"
)

# 评估OpenAI模型
evaluator_openai = SakilaText2SQLEvaluator(
    model="gpt-4",
    base_url="https://api.openai.com/v1"
)

# 比较结果
results_deepseek = evaluator_deepseek.evaluate_dataset(test_data)
results_openai = evaluator_openai.evaluate_dataset(test_data)
```

### 自定义评估

```python
# 自定义测试样本数量
test_subset = evaluator.select_test_subset(50)  # 选择50个样本

# 单独测试特定查询
result = evaluator.evaluate_single_query(
    "Show all films with their categories",
    "SELECT f.title, c.name FROM film f JOIN film_category fc ON f.film_id = fc.film_id JOIN category c ON fc.category_id = c.category_id"
)
```

## 🎓 学习价值

### 从评估中学习

1. **理解SQL复杂度**: 通过分析哪些查询容易出错，了解Text2SQL的难点
2. **模型性能分析**: 比较不同模型在不同类型查询上的表现
3. **错误模式识别**: 发现常见的SQL生成错误和改进方向

### 评估体系的意义

- **定量分析**: 提供客观的性能指标，而非主观判断
- **可重复性**: 标准化的评估流程确保结果可重现
- **迭代改进**: 为模型优化提供量化反馈

## 🔍 深入理解

### 为什么执行准确度最重要？

在Text2SQL任务中，最终目标是获得正确的查询结果。即使SQL语法不完全匹配，只要能返回正确的结果，就认为是成功的。这种"结果导向"的评估方式更符合实际应用需求。

### 评估系统的创新点

1. **多维度评估**: 不仅看语法匹配，更重视执行结果
2. **智能采样**: 确保测试集包含不同类型的SQL操作
3. **错误分析**: 详细记录每个查询的执行情况
4. **可扩展性**: 易于添加新的评估指标

## 📝 输出文件

评估系统会生成以下文件：
- `sakila_text2sql_evaluation_results_[timestamp].json`: 详细评估结果
- `text2sql_evaluation_[timestamp].log`: 评估过程日志
- `90-文档-Data/sakila.db`: Sakila数据库文件

## 🤝 贡献

这个评估系统是Text2SQL研究的重要工具。你可以：
- 添加新的评估指标
- 扩展数据库schema
- 优化评估算法
- 提供更多测试用例

## 📚 参考资料

- [Sakila数据库官方文档](https://dev.mysql.com/doc/sakila/en/)
- [Text2SQL评估方法论](https://arxiv.org/abs/1909.00218)
- [SQL语义解析评估标准](https://yale-lily.github.io/spider)

---

**作者**: RAG课程学员  
**日期**: 2024年  
**用途**: Text2SQL模型评估和RAG系统优化  
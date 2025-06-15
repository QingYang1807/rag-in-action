"""
Sakila Text2SQL 评估体系
用于评估Text2SQL模型在Sakila数据库上的性能
"""

import json
import sqlite3
import os
import sys
from typing import List, Dict, Tuple, Any
from openai import OpenAI
import numpy as np
from difflib import SequenceMatcher
import re
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'text2sql_evaluation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SakilaText2SQLEvaluator:
    """Sakila数据库Text2SQL评估器"""
    
    def __init__(self, 
                 db_path: str = "90-文档-Data/sakila.db",
                 test_data_path: str = "90-文档-Data/sakila/q2sql_pairs.json",
                 api_key: str = None,
                 model: str = "deepseek-chat",
                 base_url: str = "https://api.deepseek.com"):
        """
        初始化评估器
        
        Args:
            db_path: 数据库文件路径
            test_data_path: 测试数据JSON文件路径
            api_key: API密钥
            model: 使用的模型名称
            base_url: API基础URL
        """
        self.db_path = db_path
        self.test_data_path = test_data_path
        
        # 初始化LLM客户端
        if api_key:
            self.client = OpenAI(
                base_url=base_url,
                api_key=api_key
            )
        else:
            self.client = OpenAI(
                base_url=base_url,
                api_key=os.getenv("DEEPSEEK_API_KEY")
            )
        
        self.model = model
        self.schema_info = self._load_schema_info()
        self.test_data = self._load_test_data()
        
    def _load_schema_info(self) -> str:
        """加载数据库Schema信息"""
        schema_description = """
你正在访问Sakila电影租赁数据库，包含以下主要表：

1. actor（演员表）
   - actor_id (INT): 主键，演员唯一标识
   - first_name (VARCHAR): 演员名字
   - last_name (VARCHAR): 演员姓氏
   - last_update (TIMESTAMP): 最后更新时间

2. film（电影表）
   - film_id (INT): 主键，电影唯一标识
   - title (VARCHAR): 电影标题
   - description (TEXT): 电影描述
   - release_year (YEAR): 发行年份
   - language_id (INT): 语言ID
   - rental_duration (INT): 租赁时长（天）
   - rental_rate (DECIMAL): 租赁费用
   - length (INT): 电影时长（分钟）
   - rating (ENUM): 分级(G,PG,PG-13,R,NC-17)
   - replacement_cost (DECIMAL): 替换成本

3. category（类别表）
   - category_id (INT): 主键，类别唯一标识
   - name (VARCHAR): 类别名称

4. customer（客户表）
   - customer_id (INT): 主键，客户唯一标识
   - store_id (INT): 商店ID
   - first_name (VARCHAR): 客户名字
   - last_name (VARCHAR): 客户姓氏
   - email (VARCHAR): 电子邮箱
   - address_id (INT): 地址ID
   - active (BOOLEAN): 是否活跃
   - create_date (DATETIME): 创建日期

5. rental（租赁表）
   - rental_id (INT): 主键，租赁唯一标识
   - rental_date (DATETIME): 租赁日期
   - inventory_id (INT): 库存ID
   - customer_id (INT): 客户ID
   - return_date (DATETIME): 归还日期
   - staff_id (INT): 员工ID

6. payment（支付表）
   - payment_id (INT): 主键，支付唯一标识
   - customer_id (INT): 客户ID
   - staff_id (INT): 员工ID
   - rental_id (INT): 租赁ID
   - amount (DECIMAL): 支付金额
   - payment_date (DATETIME): 支付日期

7. inventory（库存表）
   - inventory_id (INT): 主键，库存唯一标识
   - film_id (INT): 电影ID
   - store_id (INT): 商店ID

8. staff（员工表）
   - staff_id (INT): 主键，员工唯一标识
   - first_name (VARCHAR): 员工名字
   - last_name (VARCHAR): 员工姓氏
   - address_id (INT): 地址ID
   - store_id (INT): 商店ID
   - email (VARCHAR): 电子邮箱
   - active (BOOLEAN): 是否在职

9. store（商店表）
   - store_id (INT): 主键，商店唯一标识
   - manager_staff_id (INT): 经理员工ID
   - address_id (INT): 地址ID

关系表：
- film_actor: 电影和演员的多对多关系
- film_category: 电影和类别的多对多关系
"""
        return schema_description
    
    def _load_test_data(self) -> List[Dict]:
        """加载测试数据"""
        with open(self.test_data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    def select_test_subset(self, count: int = 25) -> List[Dict]:
        """
        选择测试数据子集
        
        Args:
            count: 要选择的测试样本数量
            
        Returns:
            选中的测试数据子集
        """
        if count >= len(self.test_data):
            return self.test_data
        
        # 确保选择的数据具有代表性，包含不同类型的SQL操作
        selected_data = []
        
        # 分别选择不同类型的SQL语句
        select_queries = [item for item in self.test_data if item['sql'].strip().upper().startswith('SELECT')]
        insert_queries = [item for item in self.test_data if item['sql'].strip().upper().startswith('INSERT')]
        update_queries = [item for item in self.test_data if item['sql'].strip().upper().startswith('UPDATE')]
        delete_queries = [item for item in self.test_data if item['sql'].strip().upper().startswith('DELETE')]
        
        # 按比例选择
        select_count = min(count // 2, len(select_queries))  # 50%为SELECT
        insert_count = min(count // 6, len(insert_queries))  # 约17%为INSERT
        update_count = min(count // 6, len(update_queries))  # 约17%为UPDATE
        delete_count = count - select_count - insert_count - update_count  # 剩余为DELETE
        delete_count = min(delete_count, len(delete_queries))
        
        selected_data.extend(select_queries[:select_count])
        selected_data.extend(insert_queries[:insert_count])
        selected_data.extend(update_queries[:update_count])
        selected_data.extend(delete_queries[:delete_count])
        
        # 如果还不够，补充SELECT查询
        remaining = count - len(selected_data)
        if remaining > 0:
            remaining_selects = select_queries[select_count:select_count + remaining]
            selected_data.extend(remaining_selects)
        
        logger.info(f"选择了{len(selected_data)}个测试样本")
        logger.info(f"其中SELECT: {select_count}, INSERT: {insert_count}, UPDATE: {update_count}, DELETE: {delete_count}")
        
        return selected_data[:count]
    
    def generate_sql(self, question: str) -> str:
        """
        使用LLM生成SQL查询
        
        Args:
            question: 自然语言问题
            
        Returns:
            生成的SQL查询语句
        """
        prompt = f"""
以下是Sakila电影租赁数据库的结构描述：
{self.schema_info}

用户的自然语言问题如下：
"{question}"

请注意：
1. 请仔细分析问题涉及的表和字段
2. 考虑表之间的关联关系
3. 请只返回SQL查询语句，不要包含任何其他解释、注释或格式标记（如```sql）
4. SQL语句应该是可执行的标准SQL
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个SQL专家。请只返回SQL查询语句，不要包含任何Markdown格式或其他说明。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0
            )
            
            # 清理SQL语句
            sql = response.choices[0].message.content.strip()
            sql = sql.replace('```sql', '').replace('```', '').strip()
            return sql
            
        except Exception as e:
            logger.error(f"生成SQL时出错: {e}")
            return ""
    
    def normalize_sql(self, sql: str) -> str:
        """
        规范化SQL语句以便比较
        
        Args:
            sql: 原始SQL语句
            
        Returns:
            规范化后的SQL语句
        """
        # 转换为小写
        sql = sql.lower().strip()
        
        # 移除多余的空白字符
        sql = re.sub(r'\s+', ' ', sql)
        
        # 移除末尾的分号
        sql = sql.rstrip(';')
        
        # 统一引号
        sql = sql.replace('"', "'")
        
        return sql
    
    def sql_similarity(self, sql1: str, sql2: str) -> float:
        """
        计算两个SQL语句的相似度
        
        Args:
            sql1: 第一个SQL语句
            sql2: 第二个SQL语句
            
        Returns:
            相似度分数 (0-1)
        """
        norm_sql1 = self.normalize_sql(sql1)
        norm_sql2 = self.normalize_sql(sql2)
        
        # 使用序列匹配器计算相似度
        similarity = SequenceMatcher(None, norm_sql1, norm_sql2).ratio()
        return similarity
    
    def execute_sql(self, sql: str) -> Tuple[bool, Any]:
        """
        执行SQL语句
        
        Args:
            sql: 要执行的SQL语句
            
        Returns:
            (是否成功, 结果或错误信息)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 对于SELECT语句，返回结果
            if sql.strip().upper().startswith('SELECT'):
                cursor.execute(sql)
                results = cursor.fetchall()
                conn.close()
                return True, results
            else:
                # 对于DML语句，返回affected rows
                cursor.execute(sql)
                affected_rows = cursor.rowcount
                conn.commit()
                conn.close()
                return True, affected_rows
                
        except Exception as e:
            if 'conn' in locals():
                conn.close()
            return False, str(e)
    
    def exact_match_score(self, predicted_sql: str, reference_sql: str) -> float:
        """
        计算精确匹配分数
        
        Args:
            predicted_sql: 预测的SQL
            reference_sql: 参考SQL
            
        Returns:
            精确匹配分数 (0 或 1)
        """
        norm_pred = self.normalize_sql(predicted_sql)
        norm_ref = self.normalize_sql(reference_sql)
        return 1.0 if norm_pred == norm_ref else 0.0
    
    def execution_accuracy(self, predicted_sql: str, reference_sql: str) -> float:
        """
        计算执行准确度（结果是否相同）
        
        Args:
            predicted_sql: 预测的SQL
            reference_sql: 参考SQL
            
        Returns:
            执行准确度 (0 或 1)
        """
        pred_success, pred_result = self.execute_sql(predicted_sql)
        ref_success, ref_result = self.execute_sql(reference_sql)
        
        # 如果都执行成功且结果相同，返回1
        if pred_success and ref_success:
            if pred_result == ref_result:
                return 1.0
            else:
                return 0.0
        # 如果都执行失败，也算是一致的
        elif not pred_success and not ref_success:
            return 1.0
        else:
            return 0.0
    
    def evaluate_single_query(self, question: str, reference_sql: str) -> Dict[str, Any]:
        """
        评估单个查询
        
        Args:
            question: 自然语言问题
            reference_sql: 参考SQL
            
        Returns:
            评估结果字典
        """
        # 生成SQL
        predicted_sql = self.generate_sql(question)
        
        # 计算各种指标
        exact_match = self.exact_match_score(predicted_sql, reference_sql)
        similarity = self.sql_similarity(predicted_sql, reference_sql)
        exec_accuracy = self.execution_accuracy(predicted_sql, reference_sql)
        
        # 检查SQL是否可执行
        is_executable, exec_result = self.execute_sql(predicted_sql)
        
        result = {
            'question': question,
            'reference_sql': reference_sql,
            'predicted_sql': predicted_sql,
            'exact_match': exact_match,
            'similarity': similarity,
            'execution_accuracy': exec_accuracy,
            'is_executable': is_executable,
            'execution_result': exec_result if is_executable else str(exec_result)
        }
        
        return result
    
    def evaluate_dataset(self, test_data: List[Dict]) -> Dict[str, Any]:
        """
        评估整个数据集
        
        Args:
            test_data: 测试数据列表
            
        Returns:
            整体评估结果
        """
        results = []
        
        logger.info(f"开始评估{len(test_data)}个测试样本...")
        
        for i, item in enumerate(test_data):
            logger.info(f"评估第{i+1}/{len(test_data)}个样本...")
            
            result = self.evaluate_single_query(
                item['question'], 
                item['sql']
            )
            results.append(result)
            
            # 打印当前样本的评估结果
            logger.info(f"问题: {item['question'][:50]}...")
            logger.info(f"参考SQL: {item['sql']}")
            logger.info(f"生成SQL: {result['predicted_sql']}")
            logger.info(f"精确匹配: {result['exact_match']}")
            logger.info(f"相似度: {result['similarity']:.3f}")
            logger.info(f"执行准确度: {result['execution_accuracy']}")
            logger.info(f"是否可执行: {result['is_executable']}")
            logger.info("-" * 80)
        
        # 计算整体指标
        exact_matches = [r['exact_match'] for r in results]
        similarities = [r['similarity'] for r in results]
        exec_accuracies = [r['execution_accuracy'] for r in results]
        executabilities = [r['is_executable'] for r in results]
        
        overall_results = {
            'total_samples': len(results),
            'exact_match_accuracy': np.mean(exact_matches),
            'average_similarity': np.mean(similarities),
            'execution_accuracy': np.mean(exec_accuracies),
            'execution_success_rate': np.mean(executabilities),
            'detailed_results': results
        }
        
        return overall_results
    
    def print_evaluation_summary(self, evaluation_results: Dict[str, Any]):
        """
        打印评估结果摘要
        
        Args:
            evaluation_results: 评估结果
        """
        print("\n" + "="*60)
        print("            SAKILA TEXT2SQL 评估结果摘要")
        print("="*60)
        print(f"总测试样本数: {evaluation_results['total_samples']}")
        print(f"精确匹配准确率: {evaluation_results['exact_match_accuracy']:.3f} ({evaluation_results['exact_match_accuracy']*100:.1f}%)")
        print(f"平均SQL相似度: {evaluation_results['average_similarity']:.3f}")
        print(f"执行准确度: {evaluation_results['execution_accuracy']:.3f} ({evaluation_results['execution_accuracy']*100:.1f}%)")
        print(f"SQL可执行率: {evaluation_results['execution_success_rate']:.3f} ({evaluation_results['execution_success_rate']*100:.1f}%)")
        print("="*60)
        
        # 分析结果
        if evaluation_results['exact_match_accuracy'] >= 0.8:
            print("🎉 精确匹配准确率优秀！")
        elif evaluation_results['exact_match_accuracy'] >= 0.6:
            print("👍 精确匹配准确率良好")
        else:
            print("⚠️  精确匹配准确率需要改进")
        
        if evaluation_results['execution_accuracy'] >= 0.8:
            print("🎉 执行准确度优秀！")
        elif evaluation_results['execution_accuracy'] >= 0.6:
            print("👍 执行准确度良好")
        else:
            print("⚠️  执行准确度需要改进")
        
        if evaluation_results['execution_success_rate'] >= 0.9:
            print("🎉 SQL可执行率优秀！")
        elif evaluation_results['execution_success_rate'] >= 0.7:
            print("👍 SQL可执行率良好")
        else:
            print("⚠️  SQL可执行率需要改进")
        
        print("="*60)
    
    def save_results(self, evaluation_results: Dict[str, Any], 
                    output_file: str = None):
        """
        保存评估结果到文件
        
        Args:
            evaluation_results: 评估结果
            output_file: 输出文件路径
        """
        if output_file is None:
            output_file = f"sakila_text2sql_evaluation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(evaluation_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"评估结果已保存到: {output_file}")

def main():
    """主函数"""
    print("🚀 开始Sakila Text2SQL评估...")
    
    # 初始化评估器
    evaluator = SakilaText2SQLEvaluator()
    
    # 选择测试数据子集 (25个样本)
    test_subset = evaluator.select_test_subset(25)
    
    # 执行评估
    results = evaluator.evaluate_dataset(test_subset)
    
    # 打印结果摘要
    evaluator.print_evaluation_summary(results)
    
    # 保存结果
    evaluator.save_results(results)
    
    print("✅ 评估完成！")

if __name__ == "__main__":
    main() 
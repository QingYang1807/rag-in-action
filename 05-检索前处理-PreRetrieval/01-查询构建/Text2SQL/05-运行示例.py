"""
Sakila Text2SQL评估系统运行示例
演示如何使用评估系统评估Text2SQL模型性能
"""

import os
import sys
from datetime import datetime

# 导入我们创建的模块
from pathlib import Path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# 导入评估模块
try:
    from importlib import import_module
    db_init_module = import_module("04-Sakila-数据库初始化")
    evaluator_module = import_module("03-Sakila-Text2SQL-评估体系")
    
    create_sakila_database = db_init_module.create_sakila_database
    verify_database = db_init_module.verify_database
    SakilaText2SQLEvaluator = evaluator_module.SakilaText2SQLEvaluator
except ImportError as e:
    print(f"导入模块失败: {e}")
    print("请确保相关文件存在")
    sys.exit(1)

def SetupEnvironment():
    """设置环境和检查必要条件"""
    print("🔧 检查环境设置...")
    
    # 检查API密钥
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        print("⚠️  未找到DEEPSEEK_API_KEY环境变量")
        print("请设置环境变量: export DEEPSEEK_API_KEY=your_api_key")
        return False
    
    # 检查必要的Python包
    required_packages = ['openai', 'numpy']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"⚠️  缺少必要的Python包: {missing_packages}")
        print(f"请安装: pip install {' '.join(missing_packages)}")
        return False
    
    print("✅ 环境检查通过")
    return True

def RunQuickTest():
    """运行快速测试"""
    print("\n🚀 开始快速测试...")
    
    # 1. 初始化数据库
    print("1. 初始化Sakila数据库...")
    db_path = create_sakila_database()
    verify_database(db_path)
    
    # 2. 初始化评估器
    print("\n2. 初始化Text2SQL评估器...")
    evaluator = SakilaText2SQLEvaluator(
        db_path=db_path,
        test_data_path="90-文档-Data/sakila/q2sql_pairs.json"
    )
    
    # 3. 测试单个查询
    print("\n3. 测试单个查询...")
    test_question = "List all actors with their IDs and names."
    reference_sql = "SELECT actor_id, first_name, last_name FROM actor;"
    
    result = evaluator.evaluate_single_query(test_question, reference_sql)
    
    print(f"问题: {test_question}")
    print(f"参考SQL: {reference_sql}")
    print(f"生成SQL: {result['predicted_sql']}")
    print(f"精确匹配: {result['exact_match']}")
    print(f"相似度: {result['similarity']:.3f}")
    print(f"执行准确度: {result['execution_accuracy']}")
    print(f"是否可执行: {result['is_executable']}")
    
    print("✅ 快速测试完成！")

def RunFullEvaluation():
    """运行完整评估"""
    print("\n🚀 开始完整评估...")
    
    # 1. 确保数据库存在
    db_path = "90-文档-Data/sakila.db"
    if not os.path.exists(db_path):
        print("创建Sakila数据库...")
        create_sakila_database(db_path)
    
    # 2. 初始化评估器
    evaluator = SakilaText2SQLEvaluator(
        db_path=db_path,
        test_data_path="90-文档-Data/sakila/q2sql_pairs.json"
    )
    
    # 3. 选择测试数据子集
    test_subset = evaluator.select_test_subset(25)  # 选择25个样本
    
    # 4. 执行评估
    results = evaluator.evaluate_dataset(test_subset)
    
    # 5. 显示结果
    evaluator.print_evaluation_summary(results)
    
    # 6. 保存结果
    evaluator.save_results(results)
    
    print("✅ 完整评估完成！")

def ShowEvaluationMetrics():
    """展示评估指标说明"""
    print("\n📊 评估指标说明:")
    print("="*60)
    print("1. 精确匹配准确率 (Exact Match Accuracy):")
    print("   - 生成的SQL与标准SQL完全匹配的比例")
    print("   - 范围: 0-1，越高越好")
    print()
    print("2. 平均SQL相似度 (Average SQL Similarity):")
    print("   - 使用字符串相似度算法计算SQL语句的相似程度")
    print("   - 范围: 0-1，越高越好")
    print()
    print("3. 执行准确度 (Execution Accuracy):")
    print("   - 生成的SQL执行结果与标准SQL执行结果相同的比例")
    print("   - 范围: 0-1，越高越好")
    print("   - 这是最重要的指标，因为它关注结果正确性")
    print()
    print("4. SQL可执行率 (Execution Success Rate):")
    print("   - 生成的SQL能够成功执行（语法正确）的比例")
    print("   - 范围: 0-1，越高越好")
    print("="*60)

def main():
    """主函数"""
    print("🎉 Sakila Text2SQL评估系统")
    print("="*50)
    
    # 检查环境
    if not SetupEnvironment():
        return
    
    # 显示菜单
    while True:
        print("\n📋 选择操作:")
        print("1. 快速测试 (测试单个查询)")
        print("2. 完整评估 (评估25个样本)")
        print("3. 查看评估指标说明")
        print("4. 退出")
        
        choice = input("\n请选择 (1-4): ").strip()
        
        if choice == '1':
            RunQuickTest()
        elif choice == '2':
            RunFullEvaluation()
        elif choice == '3':
            ShowEvaluationMetrics()
        elif choice == '4':
            print("👋 再见！")
            break
        else:
            print("❌ 无效选择，请重新输入")

if __name__ == "__main__":
    main() 
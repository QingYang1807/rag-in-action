"""
Sakila Text2SQL评估结果可视化
用于生成评估结果的图表和分析报告
"""

import json
import numpy as np
from datetime import datetime
import os
from typing import Dict, List, Any

# 尝试导入可视化库
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd
    import matplotlib
    # 设置中文字体
    matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    matplotlib.rcParams['axes.unicode_minus'] = False
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

class EvaluationVisualizer:
    """评估结果可视化工具"""
    
    def __init__(self, results_file: str = None):
        """
        初始化可视化工具
        
        Args:
            results_file: 评估结果JSON文件路径
        """
        self.results_file = results_file
        self.results = None
        if results_file and os.path.exists(results_file):
            self.load_results(results_file)
    
    def load_results(self, results_file: str):
        """加载评估结果"""
        with open(results_file, 'r', encoding='utf-8') as f:
            self.results = json.load(f)
        print(f"✅ 已加载评估结果: {results_file}")
    
    def create_metrics_bar_chart(self, save_path: str = None):
        """创建评估指标柱状图"""
        if not VISUALIZATION_AVAILABLE:
            print("❌ 可视化库不可用，跳过图表生成")
            return
        
        if not self.results:
            print("❌ 未加载评估结果")
            return
        
        # 准备数据
        metrics = {
            '精确匹配准确率': self.results['exact_match_accuracy'],
            '平均SQL相似度': self.results['average_similarity'],
            '执行准确度': self.results['execution_accuracy'],
            'SQL可执行率': self.results['execution_success_rate']
        }
        
        # 创建图表
        plt.figure(figsize=(12, 8))
        bars = plt.bar(metrics.keys(), metrics.values(), 
                      color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
        
        # 添加数值标签
        for bar, value in zip(bars, metrics.values()):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{value:.3f}\n({value*100:.1f}%)', 
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        plt.title('Sakila Text2SQL 评估指标结果', fontsize=16, fontweight='bold', pad=20)
        plt.ylabel('分数', fontsize=12)
        plt.ylim(0, 1.1)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"📊 指标图表已保存: {save_path}")
        
        plt.show()
    
    def create_error_analysis(self, save_path: str = None):
        """创建错误分析图表"""
        if not VISUALIZATION_AVAILABLE:
            print("❌ 可视化库不可用，跳过图表生成")
            return
        
        if not self.results:
            print("❌ 未加载评估结果")
            return
        
        # 分析错误情况
        error_types = {
            '完全正确': 0,
            'SQL语法错误': 0,
            '执行成功但结果错误': 0,
            '执行失败': 0
        }
        
        for result in self.results['detailed_results']:
            if result['exact_match'] == 1.0:
                error_types['完全正确'] += 1
            elif not result['is_executable']:
                error_types['SQL语法错误'] += 1
            elif result['is_executable'] and result['execution_accuracy'] == 0:
                error_types['执行成功但结果错误'] += 1
            else:
                error_types['执行失败'] += 1
        
        # 创建饼图
        plt.figure(figsize=(10, 8))
        colors = ['#2ECC71', '#E74C3C', '#F39C12', '#9B59B6']
        wedges, texts, autotexts = plt.pie(error_types.values(), 
                                          labels=error_types.keys(),
                                          colors=colors,
                                          autopct='%1.1f%%',
                                          startangle=90,
                                          explode=(0.05, 0, 0, 0))
        
        plt.title('Text2SQL生成结果分析', fontsize=16, fontweight='bold', pad=20)
        
        # 美化文本
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        plt.axis('equal')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"📊 错误分析图已保存: {save_path}")
        
        plt.show()
    
    def generate_detailed_report(self, save_path: str = None):
        """生成详细的分析报告"""
        if not self.results:
            print("❌ 未加载评估结果")
            return
        
        report = []
        report.append("# Sakila Text2SQL 评估结果详细报告\n")
        report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.append("=" * 60 + "\n")
        
        # 基本统计信息
        report.append("## 基本统计信息\n")
        report.append(f"- 总测试样本数: {self.results['total_samples']}\n")
        report.append(f"- 精确匹配准确率: {self.results['exact_match_accuracy']:.3f} ({self.results['exact_match_accuracy']*100:.1f}%)\n")
        report.append(f"- 平均SQL相似度: {self.results['average_similarity']:.3f}\n")
        report.append(f"- 执行准确度: {self.results['execution_accuracy']:.3f} ({self.results['execution_accuracy']*100:.1f}%)\n")
        report.append(f"- SQL可执行率: {self.results['execution_success_rate']:.3f} ({self.results['execution_success_rate']*100:.1f}%)\n\n")
        
        # 性能等级评估
        report.append("## 性能等级评估\n")
        exec_acc = self.results['execution_accuracy']
        if exec_acc >= 0.8:
            level = "优秀 🎉"
        elif exec_acc >= 0.6:
            level = "良好 👍"
        else:
            level = "需要改进 ⚠️"
        report.append(f"- 整体性能等级: {level}\n")
        report.append(f"- 评估依据: 执行准确度 {exec_acc:.3f}\n\n")
        
        # 错误样本分析
        report.append("## 错误样本分析\n")
        error_samples = []
        perfect_samples = []
        
        for i, result in enumerate(self.results['detailed_results']):
            if result['exact_match'] == 0 or result['execution_accuracy'] == 0:
                error_samples.append((i+1, result))
            elif result['exact_match'] == 1:
                perfect_samples.append((i+1, result))
        
        report.append(f"- 错误样本数: {len(error_samples)}/{self.results['total_samples']}\n")
        report.append(f"- 完美样本数: {len(perfect_samples)}/{self.results['total_samples']}\n\n")
        
        if error_samples:
            report.append("### 典型错误案例\n")
            for i, (idx, result) in enumerate(error_samples[:5]):  # 只显示前5个
                report.append(f"**案例 {i+1} (样本 #{idx})**\n")
                report.append(f"- 问题: {result['question']}\n")
                report.append(f"- 参考SQL: `{result['reference_sql']}`\n")
                report.append(f"- 生成SQL: `{result['predicted_sql']}`\n")
                report.append(f"- 执行准确度: {result['execution_accuracy']}\n")
                report.append(f"- 是否可执行: {result['is_executable']}\n\n")
        
        # 改进建议
        report.append("## 改进建议\n")
        if self.results['execution_success_rate'] < 0.9:
            report.append("- 🔧 **语法准确性**: 生成的SQL语法错误较多，建议改进prompt或使用更好的模型\n")
        
        if self.results['execution_accuracy'] < self.results['execution_success_rate']:
            report.append("- 🎯 **语义理解**: 虽然SQL语法正确但结果不准确，需要加强数据库schema理解\n")
        
        if self.results['exact_match_accuracy'] < 0.5:
            report.append("- 📝 **SQL风格**: 生成的SQL风格与标准答案差异较大，考虑few-shot learning\n")
        
        report.append("- 📚 **数据增强**: 增加更多样化的训练样本\n")
        report.append("- 🔍 **错误分析**: 深入分析错误模式，针对性改进\n")
        
        report_text = "".join(report)
        
        if save_path:
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            print(f"📋 详细报告已保存: {save_path}")
        
        print(report_text)
        return report_text
    
    def print_summary(self):
        """打印评估摘要"""
        if not self.results:
            print("❌ 未加载评估结果")
            return
        
        print("\n" + "="*60)
        print("            SAKILA TEXT2SQL 评估结果摘要")
        print("="*60)
        print(f"总测试样本数: {self.results['total_samples']}")
        print(f"精确匹配准确率: {self.results['exact_match_accuracy']:.3f} ({self.results['exact_match_accuracy']*100:.1f}%)")
        print(f"平均SQL相似度: {self.results['average_similarity']:.3f}")
        print(f"执行准确度: {self.results['execution_accuracy']:.3f} ({self.results['execution_accuracy']*100:.1f}%)")
        print(f"SQL可执行率: {self.results['execution_success_rate']:.3f} ({self.results['execution_success_rate']*100:.1f}%)")
        print("="*60)

def main():
    """主函数"""
    print("🎨 Sakila Text2SQL 评估结果可视化工具")
    print("=" * 50)
    
    if not VISUALIZATION_AVAILABLE:
        print("⚠️ 可视化库不可用，只能生成文本报告")
        print("安装可视化库: pip install matplotlib seaborn pandas")
    
    # 查找最新的评估结果文件
    result_files = [f for f in os.listdir('.') if f.startswith('sakila_text2sql_evaluation_results_') and f.endswith('.json')]
    
    if not result_files:
        print("❌ 未找到评估结果文件")
        print("请先运行评估系统生成结果文件")
        return
    
    # 使用最新的结果文件
    latest_file = sorted(result_files)[-1]
    print(f"📁 使用评估结果文件: {latest_file}")
    
    # 初始化可视化工具
    visualizer = EvaluationVisualizer(latest_file)
    
    # 显示菜单
    while True:
        print("\n📋 选择操作:")
        print("1. 显示评估摘要")
        print("2. 生成详细报告")
        if VISUALIZATION_AVAILABLE:
            print("3. 生成指标图表")
            print("4. 生成错误分析图")
        print("0. 退出")
        
        choice = input("\n请选择: ").strip()
        
        if choice == '1':
            visualizer.print_summary()
        elif choice == '2':
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            visualizer.generate_detailed_report(f"evaluation_report_{timestamp}.md")
        elif choice == '3' and VISUALIZATION_AVAILABLE:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            visualizer.create_metrics_bar_chart(f"metrics_chart_{timestamp}.png")
        elif choice == '4' and VISUALIZATION_AVAILABLE:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            visualizer.create_error_analysis(f"error_analysis_{timestamp}.png")
        elif choice == '0':
            print("👋 再见！")
            break
        else:
            print("❌ 无效选择，请重新输入")

if __name__ == "__main__":
    main() 
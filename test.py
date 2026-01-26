import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

from dataclasses import dataclass

@dataclass
class COL:
    __col = ["A_All_change_{}", "A_2024_change_{}", "A_POIAll_change_{}"]
    all = [x.format("all") for x in __col]
    EVCS = [x.format("EVCSOnly") for x in __col]
    road = [x.format("RoadsOnly") for x in __col]
    cols = all + EVCS + road

@dataclass
class COL_GINI:
    __colGini = ["A_All_Gini_change_{}", "A_2024_Gini_change_{}", "A_POIAll_Gini_change_{}"]
    all = [x.format("all") for x in __colGini]
    EVCS = [x.format("EVCSOnly") for x in __colGini]
    road = [x.format("RoadsOnly") for x in __colGini]
    cols = all + EVCS + road


# 生成模拟数据
ANALY_RESULT = r"C:\0_PolyU\test\3km"
data = pd.read_csv(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), encoding="utf-8", usecols=["city"] + COL.cols + COL_GINI.cols).dropna()

result_AB = "A_All_change_all"
result_A_only = "A_All_change_EVCSOnly"
result_B_only = "A_All_change_RoadsOnly"

# 计算交互作用
data['interaction'] = data[result_AB] - (data[result_A_only] + data[result_B_only])
data['expected_additive'] = data[result_A_only] + data[result_B_only]

# 计算影响程度指标
def calculate_influence_metrics(df):
    """计算A和B的影响程度指标"""
    metrics = {}
    
    # 1. 主效应强度（绝对值平均）
    metrics['A_effect_strength'] = np.mean(np.abs(df[result_A_only]))
    metrics['B_effect_strength'] = np.mean(np.abs(df[result_B_only]))
    
    # 2. 主效应方向比例
    metrics['A_positive_ratio'] = np.mean(df[result_A_only] > 0)
    metrics['B_positive_ratio'] = np.mean(df[result_B_only] > 0)
    
    # 3. 交互作用强度
    metrics['interaction_strength'] = np.mean(np.abs(df['interaction']))
    metrics['synergy_ratio'] = np.mean(df['interaction'] > 0)  # 协同作用比例
    metrics['antagonism_ratio'] = np.mean(df['interaction'] < 0)  # 拮抗作用比例
    
    # 4. 相对重要性（方差解释）
    # 计算每个因素对总变异的贡献
    total_var = np.var(df[result_AB])
    # 近似：A的贡献 = 仅A结果的方差 / (仅A方差 + 仅B方差)
    var_A = np.var(df[result_A_only])
    var_B = np.var(df[result_B_only])
    metrics['A_relative_importance'] = var_A / (var_A + var_B) if (var_A + var_B) > 0 else 0
    metrics['B_relative_importance'] = var_B / (var_A + var_B) if (var_A + var_B) > 0 else 0
    
    return pd.Series(metrics)

# 计算总体指标
metrics = calculate_influence_metrics(data)
print("=== 整体影响指标 ===")
print(metrics)

# 分组分析：按交互作用类型
def analyze_by_interaction_type(df):
    """按交互作用类型分组分析"""
    results = []
    
    # 定义交互类型
    conditions = [
        ('协同作用', df['interaction'] > 0),
        ('独立作用', np.abs(df['interaction']) < 0.5),  # 阈值可调整
        ('拮抗作用', df['interaction'] < 0)
    ]
    
    for name, condition in conditions:
        subset = df[condition]
        if len(subset) > 0:
            group_metrics = calculate_influence_metrics(subset)
            group_metrics['count'] = len(subset)
            group_metrics['name'] = name
            results.append(group_metrics)
    
    return pd.DataFrame(results)

# 执行分组分析
group_analysis = analyze_by_interaction_type(data)
print("\n=== 按交互类型分组分析 ===")
print(group_analysis)

# 可视化分析
def plot_analysis_results(df):
    plt.rcParams['font.family'] = 'SimHei'
    """绘制分析图表"""
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    
    # 1. 交互作用分布
    axes[0, 0].hist(df['interaction'], bins=30, edgecolor='black', alpha=0.7)
    axes[0, 0].axvline(x=0, color='red', linestyle='--', label='零交互')
    axes[0, 0].set_xlabel('交互作用强度')
    axes[0, 0].set_ylabel('城市数量')
    axes[0, 0].set_title('交互作用分布')
    axes[0, 0].legend()
    
    # 2. 实际值 vs 加性预测值
    axes[0, 1].scatter(df['expected_additive'], df[result_AB], alpha=0.6)
    axes[0, 1].plot([df['expected_additive'].min(), df['expected_additive'].max()],
                   [df['expected_additive'].min(), df['expected_additive'].max()],
                   'r--', label='完美加性线')
    axes[0, 1].set_xlabel('加性预测值 (A+B)')
    axes[0, 1].set_ylabel('实际观测值 (AB)')
    axes[0, 1].set_title('实际值 vs 加性预测值')
    axes[0, 1].legend()
    
    # 3. 主效应散点图
    axes[0, 2].scatter(df[result_A_only], df[result_B_only], 
                      c=df['interaction'], cmap='coolwarm', alpha=0.7)
    axes[0, 2].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    axes[0, 2].axvline(x=0, color='gray', linestyle='--', alpha=0.5)
    axes[0, 2].set_xlabel('仅A影响')
    axes[0, 2].set_ylabel('仅B影响')
    axes[0, 2].set_title('主效应分布（颜色=交互作用）')
    plt.colorbar(axes[0, 2].collections[0], ax=axes[0, 2])
    
    # 4. 影响强度比较
    effect_strength = pd.DataFrame({
        'A': np.abs(df[result_A_only]),
        'B': np.abs(df[result_B_only])
    })
    axes[1, 0].boxplot([effect_strength['A'], effect_strength['B']], 
                      labels=['A影响强度', 'B影响强度'])
    axes[1, 0].set_ylabel('绝对值')
    axes[1, 0].set_title('影响强度比较')
    
    # 5. 交互作用类型分布
    interaction_types = pd.cut(df['interaction'], 
                              bins=[-np.inf, -0.5, 0.5, np.inf],
                              labels=['拮抗', '独立', '协同'])
    type_counts = interaction_types.value_counts()
    axes[1, 1].pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%')
    axes[1, 1].set_title('交互作用类型分布')
    
    # 6. 影响方向组合分析
    direction_combo = pd.crosstab(
        pd.cut(df[result_A_only], bins=[-np.inf, 0, np.inf], labels=['负', '正']),
        pd.cut(df[result_B_only], bins=[-np.inf, 0, np.inf], labels=['负', '正'])
    )
    sns.heatmap(direction_combo, annot=True, fmt='d', cmap='YlOrRd', 
                ax=axes[1, 2], cbar_kws={'label': '城市数量'})
    axes[1, 2].set_xlabel('B影响方向')
    axes[1, 2].set_ylabel('A影响方向')
    axes[1, 2].set_title('主效应方向组合')
    
    plt.tight_layout()
    plt.show()

# 执行可视化
plot_analysis_results(data)

# 统计检验
def statistical_tests(df):
    """执行统计检验"""
    print("\n=== 统计检验结果 ===")
    
    # 1. 检验A和B主效应是否显著不为0
    t_A, p_A = stats.ttest_1samp(df[result_A_only], 0)
    t_B, p_B = stats.ttest_1samp(df[result_B_only], 0)
    print(f"A主效应t检验: t={t_A:.3f}, p={p_A:.4f}")
    print(f"B主效应t检验: t={t_B:.3f}, p={p_B:.4f}")
    
    # 2. 检验交互作用是否显著不为0
    t_int, p_int = stats.ttest_1samp(df['interaction'], 0)
    print(f"交互作用t检验: t={t_int:.3f}, p={p_int:.4f}")
    
    # 3. 检验A和B影响强度的差异
    t_strength, p_strength = stats.ttest_rel(
        np.abs(df[result_A_only]), 
        np.abs(df[result_B_only])
    )
    print(f"A/B影响强度差异配对t检验: t={t_strength:.3f}, p={p_strength:.4f}")
    
    return {
        'A_pvalue': p_A,
        'B_pvalue': p_B,
        'interaction_pvalue': p_int,
        'strength_diff_pvalue': p_strength
    }

# 执行统计检验
test_results = statistical_tests(data)

# 计算相对重要性（基于贡献度分解）
def calculate_relative_importance(df):
    """计算A和B的相对重要性"""
    # 方法1：基于可分解的贡献
    total_effect = df[result_AB]
    
    # 估计在共同作用中每个因素的边际贡献
    # 假设交互作用平均分配给两个因素
    A_contribution = df[result_A_only] + 0.5 * df['interaction']
    B_contribution = df[result_B_only] + 0.5 * df['interaction']
    
    # 计算绝对贡献度
    A_abs_contrib = np.mean(np.abs(A_contribution))
    B_abs_contrib = np.mean(np.abs(B_contribution))
    
    # 相对重要性
    total = A_abs_contrib + B_abs_contrib
    if total > 0:
        A_relative = A_abs_contrib / total
        B_relative = B_abs_contrib / total
    else:
        A_relative = B_relative = 0.5
    
    importance_df = pd.DataFrame({
        'metric': ['绝对贡献度', '相对重要性'],
        'A': [A_abs_contrib, A_relative],
        'B': [B_abs_contrib, B_relative]
    })
    
    print("\n=== 因素相对重要性 ===")
    print(importance_df)
    
    return importance_df

# 计算相对重要性
importance = calculate_relative_importance(data)

# 生成综合报告
def generate_report(df, metrics, test_results):
    """生成综合分析报告"""
    print("\n" + "="*60)
    print("综合分析报告")
    print("="*60)
    
    print(f"\n1. 样本量: {len(df)}个城市")
    
    print(f"\n2. 主效应方向:")
    print(f"   A正向影响比例: {metrics['A_positive_ratio']:.1%}")
    print(f"   B正向影响比例: {metrics['B_positive_ratio']:.1%}")
    
    print(f"\n3. 影响强度:")
    print(f"   A平均绝对影响强度: {metrics['A_effect_strength']:.3f}")
    print(f"   B平均绝对影响强度: {metrics['B_effect_strength']:.3f}")
    print(f"   相对重要性 (A:B): {metrics['A_relative_importance']:.2f}:{metrics['B_relative_importance']:.2f}")
    
    print(f"\n4. 交互作用:")
    print(f"   平均交互作用强度: {metrics['interaction_strength']:.3f}")
    print(f"   协同作用比例: {metrics['synergy_ratio']:.1%}")
    print(f"   拮抗作用比例: {metrics['antagonism_ratio']:.1%}")
    
    print(f"\n5. 统计显著性 (α=0.05):")
    print(f"   A主效应显著: {'是' if test_results['A_pvalue'] < 0.05 else '否'} (p={test_results['A_pvalue']:.4f})")
    print(f"   B主效应显著: {'是' if test_results['B_pvalue'] < 0.05 else '否'} (p={test_results['B_pvalue']:.4f})")
    print(f"   交互作用显著: {'是' if test_results['interaction_pvalue'] < 0.05 else '否'} (p={test_results['interaction_pvalue']:.4f})")
    
    # 综合结论
    print(f"\n6. 主要结论:")
    if metrics['A_effect_strength'] > metrics['B_effect_strength']:
        print(f"   • 因素A的影响总体上强于因素B")
    else:
        print(f"   • 因素B的影响总体上强于因素A")
    
    if metrics['synergy_ratio'] > 0.6:
        print(f"   • A和B主要表现为协同作用")
    elif metrics['antagonism_ratio'] > 0.6:
        print(f"   • A和B主要表现为拮抗作用")
    else:
        print(f"   • A和B的作用关系复杂，协同和拮抗并存")

# 生成报告
generate_report(data, metrics, test_results)
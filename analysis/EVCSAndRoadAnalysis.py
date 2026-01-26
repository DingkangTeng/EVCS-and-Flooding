import sys
import pandas as pd
import seaborn as sns
from scipy import stats
from dataclasses import dataclass

sys.path.append(".") # Set path to the roots

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

def EVCSAndRoadAnalysis(path: str) -> None:
    df = pd.read_csv(path, encoding="utf-8", usecols=["city"] + COL.cols + COL_GINI.cols).dropna()
    cols = [COL, COL_GINI]

    import matplotlib.pyplot as plt
    for col in cols:
        for i in range(3):
            colEVCS = col.EVCS[i]
            colRoad = col.road[i]
            colAll = col.all[i]
            df["interaction"] = df[colAll] - df[colEVCS] - df[colRoad]
            df["diff"] = df[colEVCS].abs() - df[colRoad].abs()

            print(f"{col}-{i}:")

            # 创建图表
            fig, axes = plt.subplots(1, 3, figsize=(15, 5))

            # 绘制三种情景的KDE曲线
            sns.kdeplot(x=df[colEVCS], label='EVCS', ax=axes[0],
                        fill=True, alpha=0.3, color='#FF6B6B', linewidth=2)
            sns.kdeplot(x=df[colRoad], label='Road', ax=axes[0], 
                        fill=True, alpha=0.3, color='#4ECDC4', linewidth=2)
            sns.kdeplot(x=df[colAll], label='All', ax=axes[0],
                        fill=True, alpha=0.3, color='#45B7D1', linewidth=2)

            # 添加垂直线表示均值
            for scenario, color in zip([colEVCS, colRoad, colAll], 
                                    ['#FF6B6B', '#4ECDC4', '#45B7D1']):
                mean_val = df[scenario].mean()
                axes[0].axvline(mean_val, color=color, linestyle='--', alpha=0.7, 
                        label=f'{scenario}mean: {mean_val:.1f}')

            axes[0].legend()
            axes[0].grid(True, alpha=0.3)

            # 小提琴图 + 散点
            sns.violinplot(y=df['diff'], ax=axes[1], inner='box')
            sns.stripplot(y=df['diff'], ax=axes[1], color='black', alpha=0.3, size=4)
            axes[1].axhline(y=0, color='r', linestyle='--', alpha=0.5)
            axes[1].set_ylabel("EVCS Change - Road Change")

            # 直方图 + KDE
            axes[2].hist(df['diff'], bins=20, edgecolor='black', alpha=0.7, density=True)
            sns.kdeplot(df['diff'], ax=axes[2], color='red')
            axes[2].axvline(x=0, color='r', linestyle='--', alpha=0.5)
            axes[2].axvline(x=df['diff'].mean(), color='blue', linestyle='-', alpha=0.8, 
                        label=f'mean: {df["diff"].mean():.2f}')
            axes[2].set_xlabel("EVCS Change - Road Change")
            axes[2].legend()

            plt.tight_layout()
            plt.show()
    
            fig, axes = plt.subplots(1, 2, figsize=(15, 5))

            # 协同效应箱线图
            axes[0].boxplot(df['interaction'])
            axes[0].axhline(y=0, color='r', linestyle='--', alpha=0.7, linewidth=2)
            positive_pct = (df['interaction'] > 0).sum() / len(df) * 100
            axes[0].text(0.1, 0.9, f'Positive collaboration: {positive_pct:.1f}%',  #正协同
                            transform=axes[0].transAxes, fontsize=11,
                            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))

            # 散点：A效应 vs B效应，用协同效应着色
            scatter = axes[1].scatter(df[colEVCS], df[colRoad], 
                                        c=df['interaction'], cmap='RdYlBu', 
                                        alpha=0.7, edgecolors='k', s=80)
            axes[1].plot([0, max(df[colEVCS].max(), df[colRoad].max())], 
                        [0, max(df[colEVCS].max(), df[colRoad].max())], 
                        'k--', alpha=0.5)
            axes[1].set_xlabel('EVCS')
            axes[1].set_ylabel('Road')
            plt.colorbar(scatter, ax=axes[1], label='collaboration') #协同效应值

            # # 协同效应 vs 总效应
            # total_effect = df[[colEVCS, colRoad, colAll]].max(axis=1)
            # axes[2].scatter(total_effect, df['interaction'], alpha=0.6, edgecolors='k')
            # axes[2].axhline(y=0, color='r', linestyle='--', alpha=0.5)
            # axes[2].set_xlabel('max') #最大单一效应 (max(A,B))
            # axes[2].set_ylabel('collaboration') #协同效应

            plt.tight_layout()
            plt.show()

            a_wins = (df['diff'] > 0).sum()
            b_wins = (df['diff'] < 0).sum()
            t_stat, p_value = stats.ttest_rel(df[colEVCS], df[colRoad])
            # # 2. 赢家比例饼图
            # labels = ['A > B', 'A = B', 'B > A']
            # a_wins = (data['diff_AB'] > 0).sum()
            # b_wins = (data['diff_AB'] < 0).sum()
            # equal = (data['diff_AB'] == 0).sum()
            # sizes = [a_wins, equal, b_wins]
            # colors = ['#FF9999', '#CCCCCC', '#99CCFF']
            # ax2.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            # ax2.set_title('效应对比结果分布', fontsize=12, fontweight='bold')
            metrics_text = f"""
                关键分析结果：
                ────────────────
                城市总数： {len(df):d}
                ────────────────
                平均效应：
                • A单独效应： {df[colEVCS].mean():.2f}
                • B单独效应： {df[colRoad].mean():.2f}
                • 总效应： {df[colAll].mean():.2f}
                • 协同效应： {df["interaction"].mean():.2f}
                ────────────────
                影响比较：
                • A > B 的城市： {a_wins:d} ({a_wins/len(df)*100:.1f}%)
                • B > A 的城市： {b_wins:d} ({b_wins/len(df)*100:.1f}%)
                ────────────────
                统计显著性：
                • 配对t检验p值： {p_value:.4f}
                • {'B效应显著大于A' if df['diff'].mean() < 0 else 'A效应显著大于B'}
                """
            print(metrics_text)

    return

def analyze_interaction(df: pd.DataFrame, a_col='a', b_col='y', y_col='y'):
    import statsmodels.formula.api as smf
    from sklearn.preprocessing import StandardScaler
    """
    综合交互作用分析函数
    """
    import matplotlib.pyplot as plt
    from _plot import BAR_COLORS
    ax = plt.subplot()
    subset = df
    ax.scatter(
        x=subset[a_col],
        y=subset[b_col],
        # s=subset["EVCSNum"] / 10, # Point size (Million)
        # c=self.continentColors[continent],
        linewidth=1,
        # label=continent
    )
    plt.show()

    results = {}
    
    # 1. 检查可加性
    df['d'] = df[y_col] - (df[a_col] + df[b_col])
    t_stat, p_value = stats.ttest_1samp(df['d'], 0)
    results['additivity_test'] = {
        'mean_d': df['d'].mean(),
        'std_d': df['d'].std(),
        't_statistic': t_stat,
        'p_value': p_value,
        'is_additive': p_value > 0.05  # α=0.05
    }
    
    # 2. 带交互项的回归
    model = smf.ols(f'{y_col} ~ -1 + {a_col} + {b_col} + {a_col}:{b_col}', data=df).fit()
    results['regression_coefficients'] = model.params.to_dict()
    results['regression_pvalues'] = model.pvalues.to_dict()
    results['r_squared'] = model.rsquared
    
    # 3. 计算方差贡献
    # 只有A
    model_a = smf.ols(f'{y_col} ~ -1 + {a_col}', data=df).fit()
    # 只有B
    model_b = smf.ols(f'{y_col} ~ -1 + {b_col}', data=df).fit()
    # A和B（无交互）
    model_ab = smf.ols(f'{y_col} ~ -1 + {a_col} + {b_col}', data=df).fit()
    
    results['variance_contributions'] = {
        'a_only': model_a.rsquared,
        'b_only': model_b.rsquared,
        'a_and_b_no_interaction': model_ab.rsquared,
        'interaction_contribution': model.rsquared - model_ab.rsquared
    }
    
    # 4. 计算标准化系数（用于比较影响大小）
    scaler = StandardScaler()
    X_std = scaler.fit_transform(df[[a_col, b_col]])
    df_std = pd.DataFrame(X_std, columns=[f'{a_col}_std', f'{b_col}_std'])
    df_std['interaction_std'] = df_std[f'{a_col}_std'] * df_std[f'{b_col}_std']
    df_std[y_col] = df[y_col].values
    
    model_std = smf.ols(f'{y_col} ~ {a_col}_std + {b_col}_std + interaction_std', 
                       data=df_std).fit()
    
    results['standardized_coefficients'] = model_std.params.to_dict()
    
    # 5. 计算A和B的相对重要性
    a_importance = abs(results['standardized_coefficients'][f'{a_col}_std'])
    b_importance = abs(results['standardized_coefficients'][f'{b_col}_std'])
    interaction_importance = abs(results['standardized_coefficients']['interaction_std'])
    
    total_importance = a_importance + b_importance + interaction_importance
    results['relative_importance'] = {
        'a': a_importance / total_importance * 100,
        'b': b_importance / total_importance * 100,
        'interaction': interaction_importance / total_importance * 100
    }
    
    return results, model

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    # EVCSAndRoadAnalysis(os.path.join(ANALY_RESULT, "changeRatio_result.csv"))

    df = pd.read_csv(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), encoding="utf-8", usecols=["city"] + COL.cols + COL_GINI.cols).dropna()
    # 运行综合分析
    results, _ = analyze_interaction(df, "A_All_change_EVCSOnly", "A_All_change_RoadsOnly", "A_All_change_all")

    print("\n=== 综合交互作用分析结果 ===")
    print(f"\n1. 可加性检验 (p={results['additivity_test']['p_value']:.4f}):")
    print(f"   可加性假设: {'成立' if results['additivity_test']['is_additive'] else '不成立'}")

    print(f"\n2. 回归系数:")
    for var, coef in results['regression_coefficients'].items():
        p_val = results['regression_pvalues'][var]
        sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
        print(f"   {var}: {coef:.4f}{sig} (p={p_val:.4f})")

    print(f"\n3. 方差贡献:")
    for name, value in results['variance_contributions'].items():
        print(f"   {name}: {value:.4f}")

    print(f"\n4. 标准化系数:")
    for var, coef in results['standardized_coefficients'].items():
        print(f"   {var}: {coef:.4f}")

    print(f"\n5. 相对重要性 (%):")
    for var, importance in results['relative_importance'].items():
        print(f"   {var}: {importance:.2f}%")
import sys, warnings
import pandas as pd
import geopandas as gpd
import seaborn as sns
from scipy import stats
from dataclasses import dataclass

sys.path.append(".") # Set path to the roots
warnings.filterwarnings("ignore", category=UserWarning)

from _plot import plt, BAR_COLORS_TRANS, TICK_SIZE, BOX_KWARGS

@dataclass
class COL:
    __col = ["A_All_change_{}", "A_POIAll_change_{}"] #, "A_2024_change_{}"
    all = [x.format("all") for x in __col]
    EVCS = [x.format("EVCSOnly") for x in __col]
    road = [x.format("RoadsOnly") for x in __col]
    cols = all + EVCS + road

# @dataclass
# class COL_GINI:
#     __colGini = ["A_All_Gini_change_{}", "A_2024_Gini_change_{}", "A_POIAll_Gini_change_{}"]
#     all = [x.format("all") for x in __colGini]
#     EVCS = [x.format("EVCSOnly") for x in __colGini]
#     road = [x.format("RoadsOnly") for x in __colGini]
#     cols = all + EVCS + road

# _STAND_NAME = {
#     "A_All_change_EVCSOnly": "EVCS",
#     "A_All_change_RoadsOnly": "Road",
#     "A_All_change_all": "All",
#     "A_POIAll_change_EVCSOnly": "EVCS",
#     "A_POIAll_change_RoadsOnly": "Road",
#     "A_POIAll_change_all": "All",
# }

# __STAND_NAME = [
#     "EVCS\nDensity",
#     "EVCS\nCoverage",
#     "Road\nDensity", 
#     "road\nCoverage",
#     "road\nConnectivity"
# ]

__STAND_NAME = ["A", "B", "C", "D", "E"]
__INDICATOR_COLS = [
    "EVCSDensity", "EVCScoverage", "roadDensity", "roadCoverage", "roadConnectivity"
]

def __sample(df: pd.DataFrame) -> pd.DataFrame:
    def __quantileSample(group: pd.DataFrame) -> pd.DataFrame:
        import numpy as np
        MAX_POINTS_PER_GROUP = 200
        N_QUANTILES = 10

        if len(group) <= MAX_POINTS_PER_GROUP:
            return group
        # Calculate at which quantile level each point falls
        qs = np.linspace(0, 1, N_QUANTILES + 1)
        bins = group["value"].quantile(qs).values
        # Eliminate duplicate bin boundaries (there may be a large number of identical values)
        bins = np.unique(np.asarray(bins))
        group["bin"] = np.digitize(group["value"], bins, right=True)

        n = max(1, MAX_POINTS_PER_GROUP // len(np.unique(group["bin"])))
        
        return group.groupby("bin", group_keys=False)[["indicator", "value"]].apply(
            lambda g: g.sample(
                n=min(len(g), n), random_state=0
            )
        )
    
    sample = df.reset_index().melt(
        id_vars=[df.index.name or "index"],
        var_name="indicator",
        value_name="value"
    )
    
    return sample.groupby("indicator", group_keys=False)[["indicator", "value"]].apply(__quantileSample)

def EVCSAndRoadAnalysis(path: str, indicator: str) -> None:
    df = pd.read_csv(path, encoding="utf-8", usecols=["city"] + COL.cols).dropna() #  + COL_GINI.cols
    indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
        __INDICATOR_COLS
    ]
    savePath = os.path.dirname(path)

    indicatorPlots = plt.subplot("W", 1, 4, sharey=True, legend=False)
    indicatorAxs = indicatorPlots.axs
    indicatorI = 0

    for i, name in enumerate(("Population-Based", "Facility-Based")):
        colEVCS = COL.EVCS[i]
        colRoad = COL.road[i]
        colAll = COL.all[i]
        df["collaboration"] = df[colAll] - df[colEVCS] - df[colRoad]
        df["diff"] = df[colEVCS].abs() - df[colRoad].abs()

        print(f"{name}:")

        # # 创建图表
        # subplot = plt.subplot("W32", 1, 2, legend=False)
        # axs = subplot.axs

        # # 绘制三种情景的KDE曲线
        # for scenario, color in zip([colEVCS, colRoad, colAll], ['#FF6B6B', '#4ECDC4', '#45B7D1']):
        #     sns.kdeplot(x=df[scenario], label=_STAND_NAME[scenario], ax=axs[0],
        #                     fill=True, alpha=0.3, color=color, linewidth=2)

        # # 添加垂直线表示均值
        # for scenario, color in zip([colEVCS, colRoad, colAll], ['#FF6B6B', '#4ECDC4', '#45B7D1']):
        #     mean_val = df[scenario].mean()
        #     axs[0].axvline(mean_val, color=color, linestyle='--', alpha=0.7, 
        #             label=f'{_STAND_NAME[scenario]} mean: {mean_val:.1f}')

        # axs[0].set_xlabel("Index Change (%)")
        # axs[0].legend()
        # axs[0].grid(True, alpha=0.3)

        # # 直方图 + KDE
        # axs[1].hist(df['diff'], bins=20, edgecolor='black', alpha=0.7, density=True)
        # axs[1].axvline(x=0, color='r', linestyle='--', alpha=0.5)
        # axs[1].axvline(x=df['diff'].mean(), color='blue', linestyle='-', alpha=0.8, 
        #             label=f'mean: {df["diff"].mean():.2f}')
        # axs[1].set_ylabel("Density")
        # axs[1].set_xlabel("EVCS Change - Road Change")
        # axs[1].legend()

        # plt.plot(os.path.dirname(path), f"{name}_kde_diff.jpg")

        plots = plt.subplot("D", 1, 2, widthRatios=[12, 1], legend=False)
        axs = plots.axs

        # Sactter: x-EVCS, y-Road, use color to show collaboration
        scatter = axs[0].scatter(
            x=df[colEVCS], y=df[colRoad], 
            c=df["collaboration"], cmap="RdYlBu", 
            alpha=0.7,
            edgecolors="gray",
            s=80
        )
        ## y=x
        axs[0].axline((0, 0), slope=1, linestyle='--', color="red", alpha=0.5)
        ## Colorbar for collaboration
        cbar =plt.plt.colorbar(scatter, ax=axs[0], label="Collaboration")
        ## Labels
        axs[0].set_xlabel("EVCS")
        axs[0].set_ylabel("Road")

        # Boxplot for collaboration
        sns.swarmplot(
            data=__sample(df[["collaboration"]]), x="indicator", y="value",
            ax=axs[1],
            color="gray",
            alpha=0.5,
            size=3,
            zorder=1
        )
        axs[1].boxplot(
            df["collaboration"],
            positions=[0],
            patch_artist=True, showfliers=False,
            boxprops={
                "facecolor": "gray",
                "edgecolor": "gray"
            },
            medianprops = {"color": "white"},
            whiskerprops = {"color": "gray"},
            capprops = {"color": "gray"},
            zorder=2
        )
        axs[1].set_xlim(-0.2, 0.2)
        axs[1].set_xticklabels([])
        axs[1].set_xlabel("")
        axs[1].set_ylabel("")
        ## Add label for collaboration boxplot, positioned between the box and colorbar
        bboxColorBar  = cbar.ax.get_position()
        bboxBoxPlot = axs[1].get_position()
        centerX = (bboxColorBar.x0 + bboxBoxPlot.x1) / 2 + 0.06
        bottomY = min(bboxColorBar.y0, bboxBoxPlot.y0) - 0.05
        plots.fig.text(
            centerX, bottomY,
            "Collaboration",
            ha="center", va="top",
            fontdict={"size": TICK_SIZE}
        )
        ## Adjust y-axis of boxplot to match colorbar
        axs[1].set_ylim(cbar.norm.vmin, cbar.norm.vmax)
        axs[1].set_yticks(cbar.get_ticks())
        axs[1].yaxis.set_ticks_position("right")
        axs[1].yaxis.set_label_position("right")
        ## Delete ticks and label of colorbar
        cbar.ax.yaxis.set_ticks([])
        cbar.ax.set_ylabel("")

        plt.plot(savePath, f"fig3_{name}_collaboration.jpg", plots.fig)

        # Analysis metrics
        a_wins = (df['diff'] > 0).sum()
        b_wins = (df['diff'] < 0).sum()
        t_stat, p_value = stats.ttest_rel(df[colEVCS], df[colRoad])

        metrics_text = f"""
            关键分析结果：
            ────────────────
            城市总数： {len(df):d}
            ────────────────
            平均效应：
            • A单独效应： {df[colEVCS].mean():.2f}
            • B单独效应： {df[colRoad].mean():.2f}
            • 总效应： {df[colAll].mean():.2f}
            • 协同效应： {df["collaboration"].mean():.2f}
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

        # Split by dominant effect
        ## EVCS dominant cities
        evcsDominant = indicatorDf[indicatorDf.index.isin(df[df['diff'] > 0]['city'].unique())]
        print("EVCS dominant cities mean:\n", evcsDominant.mean())
        ## Roads dominant cities
        roadDominant = indicatorDf[indicatorDf.index.isin(df[df['diff'] < 0]['city'].unique())]
        print("Road dominant cities mean:\n", roadDominant.mean())

        for name2, data in zip(["EVCS Dominant", "Road Dominant"], [evcsDominant, roadDominant]):
            ax = indicatorAxs[indicatorI]
            ax.set_yscale("log")
            
            sns.swarmplot(
                data=__sample(data), x="indicator", y="value",
                ax=ax,
                color="gray",
                alpha=0.5,
                size=3,
                zorder=1,
                order=__INDICATOR_COLS # Fix indicator columns order
            )
            sns.boxplot(
                data=data,
                ax=ax,
                width=0.5,
                showmeans=True, showfliers=False,
                boxprops={
                    "facecolor": BAR_COLORS_TRANS(0.6)[indicatorI][0],
                    "edgecolor": "gray"
                },
                zorder=2,
                order=__INDICATOR_COLS, # Fix indicator columns order
                **BOX_KWARGS
            )
            
            ax.set(xlabel=f"{name} and\n{name2}", ylabel="Indicator Value")
            ax.set_xticks(range(len(__STAND_NAME)))
            ax.set_xticklabels(__STAND_NAME)

            indicatorI += 1

    plt.plot(savePath, "fig3_indicator.jpg", indicatorPlots.fig)

    return

# def analyze_collaboration(df: pd.DataFrame, a_col='a', b_col='y', y_col='y'):
#     import statsmodels.formula.api as smf
#     from sklearn.preprocessing import StandardScaler
#     """
#     综合交互作用分析函数
#     """
#     import matplotlib.pyplot as plt
#     from _plot import BAR_COLORS
#     ax = plt.subplot()
#     subset = df
#     ax.scatter(
#         x=subset[a_col],
#         y=subset[b_col],
#         # s=subset["EVCSNum"] / 10, # Point size (Million)
#         # c=self.continentColors[continent],
#         linewidth=1,
#         # label=continent
#     )
#     plt.show()

#     results = {}
    
#     # 1. 检查可加性
#     df['d'] = df[y_col] - (df[a_col] + df[b_col])
#     t_stat, p_value = stats.ttest_1samp(df['d'], 0)
#     results['additivity_test'] = {
#         'mean_d': df['d'].mean(),
#         'std_d': df['d'].std(),
#         't_statistic': t_stat,
#         'p_value': p_value,
#         'is_additive': p_value > 0.05  # α=0.05
#     }
    
#     # 2. 带交互项的回归
#     model = smf.ols(f'{y_col} ~ -1 + {a_col} + {b_col} + {a_col}:{b_col}', data=df).fit()
#     results['regression_coefficients'] = model.params.to_dict()
#     results['regression_pvalues'] = model.pvalues.to_dict()
#     results['r_squared'] = model.rsquared
    
#     # 3. 计算方差贡献
#     # 只有A
#     model_a = smf.ols(f'{y_col} ~ -1 + {a_col}', data=df).fit()
#     # 只有B
#     model_b = smf.ols(f'{y_col} ~ -1 + {b_col}', data=df).fit()
#     # A和B（无交互）
#     model_ab = smf.ols(f'{y_col} ~ -1 + {a_col} + {b_col}', data=df).fit()
    
#     results['variance_contributions'] = {
#         'a_only': model_a.rsquared,
#         'b_only': model_b.rsquared,
#         'a_and_b_no_collaboration': model_ab.rsquared,
#         'collaboration_contribution': model.rsquared - model_ab.rsquared
#     }
    
#     # 4. 计算标准化系数（用于比较影响大小）
#     scaler = StandardScaler()
#     X_std = scaler.fit_transform(df[[a_col, b_col]])
#     df_std = pd.DataFrame(X_std, columns=[f'{a_col}_std', f'{b_col}_std'])
#     df_std['collaboration_std'] = df_std[f'{a_col}_std'] * df_std[f'{b_col}_std']
#     df_std[y_col] = df[y_col].values
    
#     model_std = smf.ols(f'{y_col} ~ {a_col}_std + {b_col}_std + collaboration_std', 
#                        data=df_std).fit()
    
#     results['standardized_coefficients'] = model_std.params.to_dict()
    
#     # 5. 计算A和B的相对重要性
#     a_importance = abs(results['standardized_coefficients'][f'{a_col}_std'])
#     b_importance = abs(results['standardized_coefficients'][f'{b_col}_std'])
#     collaboration_importance = abs(results['standardized_coefficients']['collaboration_std'])
    
#     total_importance = a_importance + b_importance + collaboration_importance
#     results['relative_importance'] = {
#         'a': a_importance / total_importance * 100,
#         'b': b_importance / total_importance * 100,
#         'collaboration': collaboration_importance / total_importance * 100
#     }
    
#     return results, model

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"
    EVCSAndRoadAnalysis(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), INDICATOR)

    # df = pd.read_csv(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), encoding="utf-8", usecols=["city"] + COL.cols).dropna() # + COL_GINI.cols
    # # 运行综合分析
    # results, _ = analyze_collaboration(df, "A_All_change_EVCSOnly", "A_All_change_RoadsOnly", "A_All_change_all")

    # print("\n=== 综合交互作用分析结果 ===")
    # print(f"\n1. 可加性检验 (p={results['additivity_test']['p_value']:.4f}):")
    # print(f"   可加性假设: {'成立' if results['additivity_test']['is_additive'] else '不成立'}")

    # print(f"\n2. 回归系数:")
    # for var, coef in results['regression_coefficients'].items():
    #     p_val = results['regression_pvalues'][var]
    #     sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
    #     print(f"   {var}: {coef:.4f}{sig} (p={p_val:.4f})")

    # print(f"\n3. 方差贡献:")
    # for name, value in results['variance_contributions'].items():
    #     print(f"   {name}: {value:.4f}")

    # print(f"\n4. 标准化系数:")
    # for var, coef in results['standardized_coefficients'].items():
    #     print(f"   {var}: {coef:.4f}")

    # print(f"\n5. 相对重要性 (%):")
    # for var, importance in results['relative_importance'].items():
    #     print(f"   {var}: {importance:.2f}%")
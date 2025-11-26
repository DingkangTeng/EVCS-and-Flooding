import sys
import pandas as pd

sys.path.append(".") # Set path to the roots

from analysis.__readNode import readNode
from _plot import plotSet, plt, BAR_COLORS
from analysis.__setting import A_AFTER, A_BEFORE

def demograpicDiff(path: str, diffType: str, accOrEquity : str = "accessibility", minEvcsNum: int = 0) -> None:
    plotSet()
    df, n, nc = readNode(path, minEvcsNum, ignoreUneffected=True)

    ABefore = ["{}_Gini".format(x) for x in A_BEFORE] if accOrEquity == "equity" else A_BEFORE
    AAfter = ["{}_Gini".format(x) for x in A_AFTER] if accOrEquity == "equity" else A_AFTER
    

    diff = ["{}_diff".format(col) for col in ABefore]
    for i, col in enumerate(ABefore):
        df[diff[i]] = (df[AAfter[i]] / df[col] - 1) * 100

    fig, ax = plt.figure("H")
    
    sub, x = calDiff(df, diffType, accOrEquity)
    for i, col in enumerate(x):
        positive = sub[sub[col] > 0]
        zero = sub[sub[col] == 0]
        negative = sub[sub[col] < 0]

        ax.scatter(
            x=negative[col],
            y=negative["iso3"],
            c=BAR_COLORS[0][i]
        ) if not negative.empty else None

        ax.scatter(
            x=zero[col],
            y=zero["iso3"],
            c=BAR_COLORS[0][i],
            marker='^'
        ) if not zero.empty else None

        ax.scatter(
            x=positive[col],
            y=positive["iso3"],
            c=BAR_COLORS[0][i],
            marker='s'
        ) if not positive.empty else None

        print(f"""
            There are {len(zero)} ({len(zero)/(len(sub))*100:.2f}%) countries receive no influence from flooding in {col}
        """)

    # ax.ticklabel_format(style='plain', axis='y')
    # ax.set_xticklabels(df.index, rotation=45)
    ax.set_ylim(0, df.shape[0])

    # 创建自定义图例元素
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='gray', markersize=8, label='Negative'),
        Line2D([0], [0], marker='^', color='w', markerfacecolor='gray', markersize=8, label='Zero'),
        Line2D([0], [0], marker='s', color='w', markerfacecolor='gray', markersize=8, label='Positive')
    ]

    # 添加颜色图例（如果不同颜色代表不同含义）
    for i, col in enumerate(x):
        legend_elements.append(
            Line2D([0], [0], marker='o', color='w', markerfacecolor=BAR_COLORS[0][i], markersize=8, label=col)
        )
    
    ax.legend(handles=legend_elements, loc="lower right")
    
    plt.plot(r"C:\\0_PolyU\\test\\a.jpg")

def calDiff(df: pd.DataFrame, diffType: str, accOrEquity : str = "accessibility") -> tuple[pd.DataFrame, list[str]]:
    subdf = df.copy()
    diff = "Gini_diff" if accOrEquity == "equity" else "diff"

    if diffType == "gender":
        subdf["Male - Female"] = (subdf["A_Male_{}".format(diff)] / subdf["A_Female_{}".format(diff)] - 1) * 100
        # subdf = subdf[(subdf["Male - Female"] > -100) & (subdf["Male - Female"] < 100)]
        subdf.sort_values(by="Male - Female", inplace=True)
        x = ["Male - Female"]

    elif diffType == "age":
        x = []
        for col in ["children", "young", "elderly"]: # "A_children"
            result = "Milddle - {}".format(col.capitalize())
            subdf[result] = (subdf["A_middle_{}".format(diff)] / subdf["A_{}_{}".format(col, diff)] - 1) * 100
            # subdf = subdf[(subdf[result] > -100) & (subdf[result] < 100)]
            x.append(result)
            
    elif diffType == "evcs":
        subdf["EVCS"] = (subdf["EVCSNum_After"] / subdf["EVCSNum"] - 1) * 100
        subdf.sort_values(by="EVCS", inplace=True)
        x = ["EVCS"]

    else:
        raise RuntimeError("No corresponding group.")

    return subdf, x

if __name__ == "__main__":
    demograpicDiff(r"C:\\0_PolyU\\test\\iso3.csv", "gender", "equity")
import sys
import pandas as pd

sys.path.append(".") # Set path to the roots

from analysis.readNode import readNode
from _plot import plotSet, plt, BAR_COLORS
from analysis import A_BEFORE, A_AFTER

def demograpicDiff(path: str, diffType: str, minEvcsNum: int = 0) -> None:
    plotSet()
    df, n, nc = readNode(path, minEvcsNum, ignoreUneffected=True)
    # df.set_index("iso3", inplace=True)

    diff = ["{}_diff".format(col) for col in A_BEFORE]
    for i, col in enumerate(A_BEFORE):
        df[diff[i]] = (df[A_AFTER[i]] / df[col] - 1) * 100

    fig, ax = plt.figure("H")
    
    sub, x = calDiff(df, diffType)
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

    # plt.legend()
    
    plt.plot("test//a.jpg")

def calDiff(df: pd.DataFrame, diffType: str) -> tuple[pd.DataFrame, list[str]]:
    subdf = df.copy()

    if diffType == "gender":
        subdf["Male - Female"] = (subdf["A_Male_diff"] / subdf["A_Female_diff"] - 1) * 100
        subdf.sort_values(by="Male - Female", inplace=True)
        x = ["Male - Female"]

    elif diffType == "age":
        x = []
        for col in ["children", "young", "elderly"]: # "A_children"
            result = "Milddle - {}".format(col.capitalize())
            subdf[result] = (subdf["A_middle_diff"] / subdf["A_{}_diff".format(col)] - 1) * 100
            x.append(result)
            
    elif diffType == "evcs":
        subdf["EVCS"] = (subdf["EVCSNum_After"] / subdf["EVCSNum"] - 1) * 100
        subdf.sort_values(by="EVCS", inplace=True)
        x = ["EVCS"]

    else:
        raise RuntimeError("No corresponding group.")

    return subdf, x

if __name__ == "__main__":
    demograpicDiff(r"C:\\0_PolyU\\test\\iso3.csv", "gender")
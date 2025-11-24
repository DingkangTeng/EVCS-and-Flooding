import sys, os
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plotSet, plt
from analysis.__readNode import readNode
from analysis import A_POI, A_POP

def EVCSAnalysis(path: str, minEvcsNum: int = 10, n_bins: int = 10) -> None:
    df, n, nc = readNode(path, minEvcsNum, ignoreUneffected=True)
    df = df[df["EVCSNum"] != 0] if "city" in path else df
    idx = "city" if "city" in path else "iso3"

    plotSet()

    # Change in percentage
    df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
    df.sort_values(by="EVCSChange", inplace=True)
    print(f"The total nunmber of {path} is {n}")
    print(f"There are {df[df["EVCSChange"] == -100].shape[0]} cities/countries' EVCS are all affected by the flooding")
    print(f"There are {df[df["EVCSChange"] == 0].shape[0]} cities/countries' EVCS receive no affection from flooding.")
    fig, ax = plt.figure("D")

    # Save results
    df[[idx, "EVCSChange"]].to_csv(
        os.path.join(os.path.dirname(path), "{}_EVCS_results.csv".format(os.path.basename(path).split('.')[0])),
        encoding="utf-8",
        index=False
    )

    # Custom bins
    neg_min = np.floor(df["EVCSChange"].min() / 10) * 10
    bins = np.linspace(neg_min, 0, n_bins+1)
    evcsMax = df["EVCSChange"].max()
    if evcsMax >= 0:
        bins = np.append(bins, abs(bins[-2]))

    # Hist
    sns.histplot(
        data=df, x="EVCSChange",
        ax=ax,
        bins=bins,
        kde=False,
        color='skyblue', edgecolor='white',
        linewidth=0.5,
        stat="probability"
    )
    
    # Kernel Density Estimate
    ax2 = ax.twinx()
    sns.kdeplot(
        data=df, x="EVCSChange",
        ax=ax2, 
        color='orange',
        linewidth=1
    )

    # Set main and twin y ticks
    y1 = ax.get_yticks()
    ax.set_yticks(y1)
    y2Min, y2Max = ax2.get_ylim()
    ax2.set_yticks(np.linspace(y2Min, y2Max, len(y1)))
    ax2.grid(False)
    
    # Add mean and percentile lines
    mean_val = df["EVCSChange"].mean()
    p25 = df["EVCSChange"].quantile(0.25)
    p75 = df["EVCSChange"].quantile(0.75)

    ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'mean: {mean_val:.3f}')
    ax.axvline(p25, color='orange', linestyle=':', linewidth=1.5, alpha=0.8, label=f'25%: {p25:.3f}')
    ax.axvline(p75, color='orange', linestyle=':', linewidth=1.5, alpha=0.8, label=f'75%: {p75:.3f}')

    # sns.violinplot(
    #     data=cleandf,
    #     x = "iso3",
    #     y = "EVCSChange",
    #     ax=ax,
    #     split=True
    # )
    # plt.barh(
    #     y = range(cleandf.shape[0]),
    #     width= cleandf["EVCSChange"],
    # )
    plt.plot()
    plt.close()

    return

# Debug
if __name__ == "__main__":
    EVCSAnalysis(r"C:\\0_PolyU\\test\\city.csv")
    EVCSAnalysis(r"C:\\0_PolyU\\test\\iso3.csv")
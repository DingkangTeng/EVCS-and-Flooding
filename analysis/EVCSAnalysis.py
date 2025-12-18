import sys, os
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS
from analysis.__readNode import readNode

def EVCSAndFlooding(path: str, savePath: str = "", minEvcsNum: int = 0, nBins: int = 10) -> None:
    df, n, nc = readNode(path, minEvcsNum)
    idx = "city" if "city" in path else "iso3"

    # Change in percentage
    df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
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
    bins = np.linspace(neg_min, 0, nBins+1)
    evcsMax = df["EVCSChange"].max()
    if evcsMax >= 0:
        bins = np.append(bins, abs(bins[-2]))

    # Hist
    sns.histplot(
        data=df, x="EVCSChange",
        ax=ax,
        bins=bins,
        color=BAR_COLORS[0][0], edgecolor="white",
        linewidth=0.5,
        stat="probability"
    )
    
    # Kernel Density Estimate
    ax2 = ax.twinx()
    sns.kdeplot(
        data=df, x="EVCSChange",
        ax=ax2, 
        color=BAR_COLORS[0][1],
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
    p50 = df["EVCSChange"].quantile(0.5)

    ax.axvline(mean_val, color="red", linestyle='--', linewidth=2, label=f'Mean: {mean_val:.4f}')
    ax.axvline(p50, color="orange", linestyle=':', linewidth=1.5, alpha=0.8, label=f'Median: {p50:.4f}')
    ax.legend()

    plt.plot(savePath)

    return



# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test\\1km"
    city = os.path.join(root, "city.csv")
    iso3 = os.path.join(root, "city.csv")
    EVCSAndFlooding(city, r"C:\\0_PolyU\\test\\EVCSChange_city.jpg")
    EVCSAndFlooding(iso3, r"C:\\0_PolyU\\test\\EVCSChange_iso3")
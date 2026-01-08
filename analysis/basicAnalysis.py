import sys, os
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.ticker import FormatStrFormatter

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS
from analysis.__readNode import readNode
from analysis.__setting import STAND_NAME

def EVCSAndFlooding(path: str, savePath: str = "", minEvcsNum: int = 0, nBins: int = 10) -> pd.DataFrame:
    df, n, _ = readNode(path, minEvcsNum)
    idx = "city" if "city" in path else "iso3"

    # Change in percentage
    df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
    print(f"The total nunmber of {path} is {n}")
    print(f"There are {df[df["EVCSChange"] == -100].shape[0]} cities/countries' EVCS are all affected by the flooding")
    print(f"There are {df[df["EVCSChange"] == 0].shape[0]} cities/countries' EVCS receive no affection from flooding.")

    # Save results
    df[[idx, "EVCSChange"]].to_csv(
        os.path.join(os.path.dirname(path), "{}_EVCS_results.csv".format(os.path.basename(path).split('.')[0])),
        encoding="utf-8",
        index=False
    )

    __binPlot(df, "EVCSChange", nBins, os.path.join(savePath, "EVCSChange_{}.jpg".format(idx)))

    return df[[idx, "EVCSChange"]]

def otherIndicator(
    path: tuple[str, str],
    indicators: list[str], idx: str = "city",
    EVCSChange: pd.DataFrame | None = None,
    savePath: str = "",
    minEvcsNum: int = 0, nBins: int = 10
) -> None:
    df, _, _ = readNode(path, minEvcsNum)

    if EVCSChange is not None and "EVCSChange" not in df.columns:
        df = df.merge(EVCSChange, left_on="city", right_on="city", how="left")
        df.rename(columns={"EVCSChange_y": "EVCSChange"}, inplace=True)
        df["EVCSChange"] = df["EVCSChange"] / 100
        df.to_file(path[0], layer=path[1], encoding="utf-8")

    for col in indicators:
        if "overage" in col: df[col] *= 100 # Coverage or coverage

        __binPlot(df, col, nBins, os.path.join(savePath, "{}_{}.jpg".format(col, idx)))

def __binPlot(df: pd.DataFrame, col: str, nBins: int, savePath: str = "") -> None:
    fig, ax = plt.figure("D31")

    # Custom bins
    evcsMax = df[col].max()
    negMin = np.floor(df[col].min() / 10) * 10
    if evcsMax >= 0 and col == "EVCSChange":
        bins = np.linspace(negMin, 0, nBins+1)
        bins = np.append(bins, abs(bins[-2]))
    else:
        bins = np.linspace(negMin, np.ceil(evcsMax / 10) * 10, nBins+1)

    # Hist
    sns.histplot(
        data=df, x=col,
        ax=ax,
        bins=bins,
        color=BAR_COLORS[0][0], edgecolor="white",
        linewidth=0.5,
        stat="probability"
    )

    ax.set_xlabel(STAND_NAME[col])
    
    # Kernel Density Estimate
    ax2 = ax.twinx()
    sns.kdeplot(
        data=df, x=col,
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
    formatter = FormatStrFormatter("%.4f")
    ax2.yaxis.set_major_formatter(formatter)
    
    # Add mean and percentile lines
    mean = df[col].mean()
    p50 = df[col].quantile(0.5)

    ax.axvline(mean, color="red", linestyle='--', linewidth=2, label=f'Mean: {mean:.2f}')
    ax.axvline(p50, color="orange", linestyle=':', linewidth=1.5, alpha=0.8, label=f'Median: {p50:.2f}')
    ax.legend()

    plt.plot(savePath)

# Debug
if __name__ == "__main__":
    ANALY_RESULT_ROOT = r"C:\\0_PolyU\\test"
    CITY_RESULT = os.path.join(ANALY_RESULT_ROOT, "3km", "city.csv")
    INDICATOR = os.path.join(ANALY_RESULT_ROOT, "indicator.gpkg")

    EVCSchange = EVCSAndFlooding(CITY_RESULT, ANALY_RESULT_ROOT)
    otherIndicator((INDICATOR, "city"), ["EVCScoverage", "folldingCoverage"], EVCSChange=EVCSchange, savePath=ANALY_RESULT_ROOT)
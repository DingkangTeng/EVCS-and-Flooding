import sys, os
import pandas as pd
import numpy as np
from scipy import stats
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.pyplot import scatter

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS, NOTE_SIZE
from analysis.__readNode import readNode
from analysis.__setting import AColumns
from analysis.__calRatio import calRatio

_STAND_NAME = {
    "A_All_changeresultCols": "accessibility",
    "A_2024_changeresultCols": "accessibility",
    "A_POIAll_changeresultCols": "accessibility",
    "A_All_Gini_changeresultCols": "Gini coefficient",
    "A_2024_Gini_changeresultCols": "Gini coefficient",
    "A_POIAll_Gini_changeresultCols": "Gini coefficient",
    "EVCS_Change": "EVCS count"
}

class accAndEquityAnalysis:
    __slots__ = ["df", "ratio", "continent", "scale", "savePath"]

    def __init__(self, path: str, analysisType: str, savePath: str, minEvcsNum: int = 0) -> None:
        df, n, nc = readNode(path, minEvcsNum)
        df["EVCS_Change"] = (df["EVCSNum_After"] / df["EVCSNum"] - 1) * 100

        A_ACC_BEFORE, A_ACC_AFTER = AColumns(analysisType, "accessibility")
        A_EQU_BEFORE, A_EQU_AFTER = AColumns(analysisType, "equity")
        # Only consider the overall demograpic/POI
        A_BEFORE = [A_ACC_BEFORE[-1], A_EQU_BEFORE[-1]]
        A_AFTER = [A_ACC_AFTER[-1], A_EQU_AFTER[-1]]
        self.scale = os.path.basename(path).split('.')[0]
        addCol = ["iso3", "city", "EVCSNum", "EVCS_Change"] if self.scale == "city" else ["iso3", "EVCSNum"]
        df = df[A_BEFORE + A_AFTER + addCol]
        self.savePath = os.path.join(savePath, analysisType)
        if not os.path.exists(self.savePath): os.makedirs(self.savePath)

        df, ratio = calRatio(df, A_BEFORE, A_AFTER)
        df: pd.DataFrame = df[addCol + ratio.tolist()]

        continent = pd.read_csv(r"analysis\dcw-countries.csv", usecols=["iso3", "continent"], index_col="iso3",)
        self.df: pd.DataFrame = df.join(continent, on="iso3")
        self.df.dropna(inplace=True)
        self.df = self.df[(self.df[ratio[0]] > -99) & (self.df[ratio[1]] > -99)]
        self.ratio = ratio
        self.continent = self.df["continent"].unique()
        del df

        return
    
    def identifyOutliers(self, threshold: float) -> None:
        """
        Recognize outlier cities based on Z-score method.
        Default threshold is 2 standard deviations.
        """
        # z-score outlier
        self.df["z1"] = np.abs(stats.zscore(self.df[self.ratio[0]]))
        self.df['z2'] = np.abs(stats.zscore(self.df[self.ratio[1]]))
        self.df["outlier"] = (self.df["z1"] > threshold) | (self.df["z2"] > threshold)
        
        # Calculate Euclidean distance from origin
        self.df["d"] = np.sqrt(
            self.df[self.ratio[0]]**2 + self.df[self.ratio[1]]**2
        )
        distanceThreshold = np.percentile(self.df["d"], 90)
        self.df["extreme"] = self.df["d"] > distanceThreshold
        
        return
    
    def plot(self, figsize: str = "W", threshold: float = 3) -> None:
        # self.identifyOutliers(threshold)
        subpltos = plt.subplot(figsize, 1 ,2, [1, 1])

        # Assign colors based on continent
        continentColors = {
            x: y for x, y in zip(self.continent, BAR_COLORS[0] + BAR_COLORS[1])
        }
        self.df["color"] = self.df["continent"].map(continentColors)
        
        # Plot, the point size is based on EVCS number
        result = ""
        pairs = [(self.ratio[0], self.ratio[1]), (self.ratio[0], "EVCS_Change")]
        for i, ax in enumerate(subpltos.axs):
            x, y = pairs[i]
            rstr, corr = self.outputInformation(x, y)
            result += f"Relation ship between {_STAND_NAME.get(x)} and {_STAND_NAME.get(y)}:\n{rstr}"

            for continent in self.continent:
                subset = self.df[self.df["continent"] == continent]
                ax.scatter(
                    x=subset[x],
                    y=subset[y],
                    s=subset["EVCSNum"] / 10, # Point size (Million)
                    c=continentColors[continent],
                    linewidth=1,
                    label=continent
                )
            
            ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
            ax.axvline(x=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
            
            # Add trend line
            z = np.polyfit(self.df[x], self.df[y], 1)
            p = np.poly1d(z)
            xTrend = np.linspace(self.df[x].min(), self.df[x].max(), 100)
            ax.plot(xTrend, p(xTrend), color="black", linewidth=2, linestyle='-', alpha=0.7, label="Overall Trend Line")
            
            # # Label outlier cities
            # outliers = self.df[self.df["outlier"]]
            # for _, row in outliers.iterrows():
            #     ax.annotate(
            #         row[self.scale],
            #         xy=(row[self.ratio[0]], row[self.ratio[1]]),
            #         xytext=(5, 5),
            #         textcoords="offset points",
            #         fontsize=NOTE_SIZE,
            #         fontweight="bold",
            #         bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
            #         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2', color='red', alpha=0.6)
            #     )
            
            ax.set_xlabel("Change in {} (%)".format(_STAND_NAME.get(x, x)))
            ax.set_ylabel("Change in {} (%)".format(_STAND_NAME.get(y, y)))

            # R2
            yPred = p(self.df[x])
            yMean = np.mean(self.df[y])
            res = np.sum((self.df[y] - yPred) ** 2)
            tot = np.sum((self.df[y] - yMean) ** 2)
            r2 = 1 - res / tot
            ## Add text
            ax.text(
                0.95, 0.10,
                f"R² = {r2:.4f}\nCorrelation = {corr:.4f}", 
                transform=ax.transAxes, 
                fontsize=NOTE_SIZE,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(boxstyle="round", facecolor="#FFFFFF", alpha=0.8)
            )

        # Add continent legends
        fig = subpltos.fig
        legends = []

        # Continent
        legends.extend([
            Patch(color=color, label=continent) 
            for continent, color in continentColors.items()
        ])

        # EVCS
        smin = int(self.df["EVCSNum"].min())
        smax = int(self.df["EVCSNum"].max() / 100) * 100
        smid = int(((smin + smax) >> 1) / 100) * 100
        sizes = [smin, smid, smax]

        for size in sizes:
            legends.append(
                scatter([], [], s=size/10, c="gray",
                        linewidth=1, label=f"EVCS: {size}")
            )

        # trends
        legends.append(
            Line2D([], [], color="black", linewidth=2, 
                linestyle="-", label="Trend Line")
        )

        fig.legend(
            handles=legends,
            loc="lower center",
            ncol=len(continentColors) + 1,
            frameon=True,
            fancybox=True,
            borderpad=1,
            labelspacing=0.5,
            handletextpad=1,
            columnspacing=1.5
        )

        plt.plot(os.path.join(self.savePath, f"{self.scale}_Acc&EquCompare.jpg"))
        with open(os.path.join(self.savePath, f"{self.scale}_Acc&EquCompare.txt"), 'w') as f:
            f.write(result)

        return

    def outputInformation(self, x: str, y: str) -> tuple[str, float]:
        # print("\n=== Outlier cities ===")
        # outliers = self.df[self.df["outlier"]]
        # print(f"The number of outlire cities is {outliers.shape[0]}")
        # print(outliers.to_string(index=False))

        result = "=== Statistic information by continents ===\n"
        summary = self.df.groupby("continent").agg({
            x: ["mean", "std", "min", "max"],
            y: ["mean", "std", "min", "max"],
            self.scale: "count"
        }).round(2)
        summary.columns = [
            " ".join(
                [str(_STAND_NAME.get(x, x)) for x in col if x not in {None, "", " "}]
            ) for col in summary.columns.to_flat_index()
        ]
        result += summary.to_string(justify="center")

        # correlation coefficient
        correlation = self.df[x].corr(self.df[y])
        result += f"\n\nThe correlation coefficient between {_STAND_NAME.get(x)} and {_STAND_NAME.get(y)} is: {correlation:.4f}"

        # correlation coefficient by continents
        result += "\n\n=== correlation coefficient by continents ==="
        for continent in self.continent:
            subset = self.df[self.df["continent"] == continent]
            if len(subset) > 1:
                corr = subset[x].corr(subset[y])
                result += f"\n{continent}: {corr:.4f} (Total: {len(subset)})"

        result += "\n\n"

        return result, correlation

if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    ANALY_RESULT = os.path.join(root, "3km")
    CITY_RESULT = os.path.join(ANALY_RESULT, "city.csv")
    accAndEquityAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT).plot()
    accAndEquityAnalysis(CITY_RESULT, "popDynamic", ANALY_RESULT).plot()
    accAndEquityAnalysis(CITY_RESULT, "POI", ANALY_RESULT).plot()
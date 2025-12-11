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
from analysis.__setting import STAND_NAME, AColumns
from analysis.__calRatio import calRatio

class accAndEquityAnalysis:
    __slots__ = ["df", "ratio", "continent", "scale", "savePath"]

    def __init__(self, path: str, analysisType: str, savePath: str, minEvcsNum: int = 0) -> None:
        df, n, nc = readNode(path, minEvcsNum)

        A_ACC_BEFORE, A_ACC_AFTER = AColumns(analysisType, "accessibility")
        A_EQU_BEFORE, A_EQU_AFTER = AColumns(analysisType, "equity")
        # Only consider the overall demograpic/POI
        A_BEFORE = [A_ACC_BEFORE[-1], A_EQU_BEFORE[-1]]
        A_AFTER = [A_ACC_AFTER[-1], A_EQU_AFTER[-1]]
        self.scale = os.path.basename(path).split('.')[0]
        addCol = ["iso3", "city", "EVCSNum"] if self.scale == "city" else ["iso3", "EVCSNum"]
        df = df[A_BEFORE + A_AFTER + addCol]
        self.savePath = os.path.join(savePath, analysisType)
        if not os.path.exists(savePath): os.makedirs(savePath)

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
    
    def plot(self, figsize: str = "D", threshold: float = 3) -> None:
        self.identifyOutliers(threshold)
        fig, ax = plt.figure(figsize)

        # Assign colors based on continent
        continentColors = {
            x: y for x, y in zip(self.continent, BAR_COLORS[0] + BAR_COLORS[1])
        }
        self.df["color"] = self.df["continent"].map(continentColors)
        
        # Plot, the point size is based on EVCS number
        for continent in self.continent:
            subset = self.df[self.df["continent"] == continent]
            ax.scatter(
                x=subset[self.ratio[0]],
                y=subset[self.ratio[1]],
                s=subset["EVCSNum"] / 10, # Point size (Million)
                c=continentColors[continent],
                linewidth=1,
                label=continent
            )
        
        ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        ax.axvline(x=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        
        # Add trend line
        z = np.polyfit(self.df[self.ratio[0]], self.df[self.ratio[1]], 1)
        p = np.poly1d(z)
        xTrend = np.linspace(self.df[self.ratio[0]].min(), self.df[self.ratio[0]].max(), 100)
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
        
        # Add continent legends
        continentPatches = [
            Patch(color=color, label=continent) for continent, color in continentColors.items()
        ]
        leg1 = ax.legend(handles=continentPatches, title="Continent", loc="upper left", bbox_to_anchor=(1.02, 1))
        ax.add_artist(leg1)
        
        # Add EVCS size legends
        smin = int(self.df["EVCSNum"].min())
        smax = int(self.df["EVCSNum"].max() / 100) * 100
        smid = int(((smin + smax) >> 1) / 100) * 100
        sizes = [smin, smid, smax]  # EVCS size
        legendElements = []
        for size in sizes:
            legendElements.append(
                scatter(
                    [], [], s=size/10, c="gray",
                    linewidth=1, label=f"{size}"
                )
            )
        leg2 = ax.legend(
            handles=legendElements, title="EVCS number", loc="upper left", 
            bbox_to_anchor=(1.02, 0.68), labelspacing=1
        )
        ax.add_artist(leg2)
        
        # Add trend line legend
        trendLine = Line2D([], [], color="black", linewidth=2, linestyle="-", label="Trend Line")
        ax.legend(handles=[trendLine], loc="upper left", bbox_to_anchor=(1.02, 0.44))
        
        ax.set_xlabel("Change in accessibility(%)")
        ax.set_ylabel("Change in equity (%)")
        
        plt.plot(os.path.join(self.savePath, f"{self.scale}_Acc&EquCompare.jpg"))
        self.outputInformation()

        return

    def outputInformation(self) -> None:
        # print("\n=== Outlier cities ===")
        # outliers = self.df[self.df["outlier"]]
        # print(f"The number of outlire cities is {outliers.shape[0]}")
        # print(outliers.to_string(index=False))

        print("\n=== Statistic information by continents ===")
        summary = self.df.groupby("continent").agg({
            self.ratio[0]: ["mean", "std", "min", "max"],
            self.ratio[1]: ["mean", "std", "min", "max"],
            self.scale: "count"
        }).round(2)
        print(summary)

        # Correlation index
        correlation = self.df[self.ratio[0]].corr(self.df[self.ratio[1]])
        print(f"\nThe correaltion index between {self.ratio[0]} and {self.ratio[1]} is: {correlation:.4f}")

        # 按大洲计算相关系数
        print("\n=== Correlation index by continents ===")
        for continent in self.continent:
            subset = self.df[self.df["continent"] == continent]
            if len(subset) > 1:
                corr = subset[self.ratio[0]].corr(subset[self.ratio[1]])
                print(f"{continent}: {corr:.4f} (Total: {len(subset)})")

if __name__ == "__main__":
    accAndEquityAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popStatic", r"C:\\0_PolyU\\test").plot()
    accAndEquityAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popDynamic", r"C:\\0_PolyU\\test").plot()
    accAndEquityAnalysis(r"C:\\0_PolyU\\test\\city.csv", "POI", r"C:\\0_PolyU\\test").plot()
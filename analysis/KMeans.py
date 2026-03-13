import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import seaborn as sns
from matplotlib.ticker import PercentFormatter
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import silhouette_score

sys.path.append(".") # Set path to the roots

from _function.readChangeRatio import readChangeRatio
from _function.readFiles import mkdir
from _plot import plt, BAR_COLORS

class clustering:
    __slots__ = [
        "df", "COL", "cities",
        "Xscaled", "savePath"
    ]

    __CLUST_INDICATOR_COLS = [
        "EVCSAggregation", "roadDensity", "folldingCoverage"
    ]

    __CLUST_STAND_NAME = [
        "EVCS aggregation (NNI)", "Road density (km/km²)", "Flooding coverage (%)"
    ]
    
    __INDICATOR_COLS = [
        "EVCSDensity", "EVCSAggregation",
        "EVCSConnectivity", "EVCSChange", "EVCSPop"
    ]

    __STAND_NAME = [
        "EVCS\ndensity", "EVCS\naggregation\n(NNI)",
        "Average\nroad counts\nper EVCS", "EVCS\nchange ratio", "Per capita\nnumber of\nEVCS"
    ]

    def __init__(self, path: str, indicator: str, savePath: str) -> None:
        self.df, self.COL = readChangeRatio(path, indexCity=True, initial=True)
        indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
            list(set(self.__INDICATOR_COLS) | set(self.__CLUST_INDICATOR_COLS))
        ]
        self.df = self.df.join(indicatorDf)
        
        # Get data for clustering
        indicatorDf = indicatorDf[self.__CLUST_INDICATOR_COLS]
        scaler = MinMaxScaler()
        self.Xscaled = scaler.fit_transform(indicatorDf.to_numpy())
        # self.df[self.__INDICATOR_COLS] = scaler.fit_transform(self.df[self.__INDICATOR_COLS])
        self.cities = indicatorDf.index
        self.savePath = os.path.join(savePath, "clustering")
        mkdir(self.savePath)

        # # No correlation
        # from analysis.regression import autoCorrelation
        # autoCorrelation(self.df, self.__INDICATOR_COLS, [], True)

        del indicatorDf

        return

    def compareK(self) -> None:
        plots = plt.subplot("W", 1, 2, legend=False)
        axs = plots.axs

        sse = []
        sil_scores = []
        for k in range(1, 10):
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(self.Xscaled)
            sse.append(kmeans.inertia_)

            if k == 1: continue
            labels = kmeans.fit_predict(self.Xscaled)
            sil_scores.append(silhouette_score(self.Xscaled, labels))


        axs[0].plot(range(1,10), sse, marker='o')
        axs[0].set_xlabel('k')
        axs[0].set_ylabel("SSE")
            
        axs[1].plot(range(2,10), sil_scores, marker='o')
        axs[1].set_xlabel('k')
        axs[1].set_ylabel('Silhouette Score')

        plt.plot(self.savePath, "appendix_compare_K.jpg")
        
        return

    def run(self, k: int) -> None:
        kmeans = KMeans(n_clusters=k, random_state=42)
        labels = kmeans.fit_predict(self.Xscaled)
        df = pd.DataFrame(labels, columns=["cluster"], index=self.cities).join(self.df)
        clusters = df["cluster"].unique()
        clusters.sort()

        # Clusting results
        plots = plt.subplot("W", 1, 2, sharey=True)
        for i in range(0, 2):
            ax = plots.axs[i]
            sns.scatterplot(
                data=df,
                x=self.__CLUST_INDICATOR_COLS[i+i], y=self.__CLUST_INDICATOR_COLS[1],
                ax=ax,
                hue="cluster", palette="Set1"
            )
            ax.set_xlabel(self.__CLUST_STAND_NAME[i+i])
            ax.set_ylabel(self.__CLUST_STAND_NAME[1])
        plots.legend(title="Clustering number", ncol=k)
        plt.plot(self.savePath, "clustering_result.jpg")

        ## Save clusting results as table
        df[["cluster"] + self.__CLUST_INDICATOR_COLS].groupby("cluster").agg(
            ["mean", "median"]
        ).to_csv(
            os.path.join(self.savePath, "clustering_result.csv"),
            float_format="%.4f"
        )
        
        # Different influence in different group
        portionPlots = plt.subplot("W", 1, 2, sharey=False)
        portionAxs = portionPlots.axs
        N = len(clusters)
        radarPlots = plt.subplot("W", 2, N, legend=False, projection="polar")
        # radarPlots = plt.subplot("W", 1, 2, projection="polar")
        radarAxs = radarPlots.axs

        for i, name in enumerate(("Population-based", "Facility-based")):
            colEVCS = self.COL.EVCS[i]
            colRoad = self.COL.road[i]
            colAll = self.COL.all[i]
            colOrigion = "A_All" if name == "Population-based" else "A_POIAll"
            # featureCols = [colEVCS, colRoad, colAll, "diff", "winner"] + self.__CLUST_INDICATOR_COLS

            df["diff"] = df[colEVCS].abs() - df[colRoad].abs()
            df["winner"] = "road"
            df.loc[df["diff"] > 0, "winner"] = "EVCS"
            df.loc[df["diff"] == 0, "winner"] = "tie"

            # # Group by clustering and different winner
            # countTable = df.groupby(["cluster", "winner"]).size().unstack(fill_value=0)
            
            # # Count the total number and proportion
            # countTable["total"] = countTable.sum(axis=1)
            # countTable["roadRatio"] = countTable["road"] / countTable["total"]
            # countTable["EVCSRatio"] = countTable["EVCS"] / countTable["total"]
            
            # print(f"\n--- Statistic ---")
            # print(countTable)

            # Plot
            ax = portionAxs[i]
            ## Cross table
            ct = pd.crosstab(df["cluster"], df["winner"])
            ct = ct.div(ct.sum(axis=1), axis=0)  # Normalize by row
            ct.plot(kind="bar", stacked=True, ax=ax, color=BAR_COLORS[0])
            ax.set_xlabel("Cluster")
            ax.set_ylabel("Proportion (%)")
            ax.yaxis.set_major_formatter(PercentFormatter(1.0, symbol=None))  # Change y axis to percentage

            # Different cluster
            for j, cluster in enumerate(clusters):
                clusterData: pd.DataFrame = df[df["cluster"] == cluster]
                clusterData = clusterData.sort_values(colOrigion, ascending=False).head(int(np.ceil(clusterData.shape[0] * 0.5)))
                nTop = int(np.ceil(clusterData.shape[0] * 0.1))
                top = clusterData.nlargest(nTop, colAll)
                # Cover all zero change city if n top are all zero
                if top[colAll].iat[-1] == 0:
                    top = clusterData[clusterData[colAll] == 0]
                bottom = clusterData.nsmallest(nTop, colAll)
                # Exclude zero data if n bottom contain zero
                bottom = bottom[bottom[colAll] != 0]

                # # 对初始可达性降序排名（值越大，排名数字越小）
                # origion = df[colOrigion].rank(ascending=False, method="min")
                # # 对变化程度升序排名（值越大越好）
                # change = df[colAll].rank(ascending=True, method='min')
                # # 综合排名（等权重求和）
                # rankDf = df.copy()
                # rankDf["rank"] = origion + change

                # # 按综合排名升序排序
                # rankDf = rankDf.sort_values("rank")

                # # 前n韧性强的城市
                # top = rankDf.head(nTop)

                # # 后n韧性弱的城市（综合排名最大的n个）
                # bottom = rankDf.tail(nTop)

                # print(f"\n簇 {cluster} - {name} ：")
                # print(pd.concat([top[featureCols], bottom[featureCols]]).to_string())

                # goodAvg = self.Xscaled[df.index.get_indexer(top.index), :].mean(axis=0)
                # badAvg = self.Xscaled[df.index.get_indexer(bottom.index), :].mean(axis=0)
                goodAvg = top[self.__INDICATOR_COLS].mean(axis=0)
                badAvg = bottom[self.__INDICATOR_COLS].mean(axis=0)
                diff = np.select(
                    [
                        (goodAvg == 0) & (badAvg == 0),
                        (goodAvg != 0) & (badAvg == 0),
                        (badAvg != 0)
                    ], [
                        0,  # 两者均为0
                        1,  # 仅badAvg为0
                        np.clip(goodAvg / badAvg - 1, None, 1)  # badAvg非0，计算后截断上限
                    ]
                )
                # print(goodAvg)
                # print(badAvg)
                # print(diff)

                ax = radarAxs[i*N+j]
                # self.__plotRadar(radarAxs[i*N+j], goodAvg.to_numpy(), self.__INDICATOR_COLS, BAR_COLORS[0][1], "Good")
                # self.__plotRadar(radarAxs[i*N+j], badAvg.to_numpy(), self.__INDICATOR_COLS, BAR_COLORS[0][2], "Bad")
                self.__plotRadar(ax, diff, self.__INDICATOR_COLS, BAR_COLORS[0][j], cluster)
                ax.yaxis.set_major_formatter(PercentFormatter(1.0))  # Change y axis to percentage
                ax.set_xticklabels(self.__STAND_NAME)

        portionPlots.legend(ncol=k)
        plt.plot(self.savePath, "clusting_portion", fig=portionPlots.fig)

        radarPlots.legend(ncol=2)
        plt.plot(self.savePath, "clusting_radar", fig=radarPlots.fig)
        
        return
    
    @staticmethod
    def __plotRadar(ax: plt.Axes, data: np.ndarray, categories: list, color: str, label: str) -> None:
        N = len(categories)
        angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
        data = np.concatenate((data, [data[0]]))  # Close data for radar
        angles += angles[:1]
        ax.plot(angles, data, 'o-', linewidth=2, color=color, label=label)
        ax.fill(angles, data, alpha=0.25, color=color)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.axhline(y=0, color="red")
        ax.set_ylim(-1, 1)

        return

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"

    a = clustering(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), INDICATOR, savePath=ANALY_RESULT)
    # a.compareK()
    a.run(k=4)
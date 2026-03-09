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
        "scaler", "Xscaled", "savePath"
    ]

    __INDICATOR_COLS = [
        "EVCSAggregation", "roadDensity", "folldingCoverage"
    ]
    __STAND_NAME = [
        "EVCS aggregation (NNI)", "Road density (km/km²)", "Flooding coverage (%)"
    ]

    def __init__(self, path: str, indicator: str, savePath: str) -> None:
        self.df, self.COL = readChangeRatio(path, indexCity=True)
        indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
            self.__INDICATOR_COLS
        ]
        self.df = self.df.join(indicatorDf)
        
        self.scaler = MinMaxScaler()
        self.Xscaled = self.scaler.fit_transform(indicatorDf.to_numpy())
        self.cities = indicatorDf.index
        self.savePath = os.path.join(savePath, "clustering")
        mkdir(self.savePath)

        # No correlation
        # from analysis.regression import autoCorrelation
        # autoCorrelation(indicatorDf, self.__INDICATOR_COLS, [], True)

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
        plots = plt.subplot("W", 1, 2)
        for i in range(0, 2):
            ax = plots.axs[i]
            sns.scatterplot(
                data=df,
                x=self.__INDICATOR_COLS[0+i], y=self.__INDICATOR_COLS[1+i],
                ax=ax,
                hue="cluster", palette="Set1"
            )
            ax.set_xlabel(self.__STAND_NAME[0+i])
            ax.set_ylabel(self.__STAND_NAME[1+i])
        plots.legend(title="Clustering number", ncol=k)
        plt.plot(self.savePath, "clustering_result.jpg")
        
        # Different influence in different group
        portionPlots = plt.subplot("W", 1, 2, sharey=False)
        portionAxs = portionPlots.axs
        N = len(clusters)
        radarPlots = plt.subplot("W", 2, N, projection="polar")
        radarAxs = radarPlots.axs

        for i, name in enumerate(("Population-based", "Facility-based")):
            colEVCS = self.COL.EVCS[i]
            colRoad = self.COL.road[i]
            colAll = self.COL.all[i]
            featureCols = [colEVCS, colRoad, colAll, "diff", "winner"] + self.__INDICATOR_COLS

            df["diff"] = df[colEVCS].abs() - df[colRoad].abs()

            # 定义赢家类别
            df["winner"] = "road"
            df.loc[df["diff"] > 0, "winner"] = "EVCS"
            df.loc[df["diff"] == 0, "winner"] = "tie"  # 如有平局

            # 按簇和赢家分组计数
            count_table = df.groupby(["cluster", "winner"]).size().unstack(fill_value=0)
            
            # 添加总计和比例
            count_table["total"] = count_table.sum(axis=1)
            count_table["roadRatio"] = count_table["road"] / count_table["total"]
            count_table["EVCSRatio"] = count_table["EVCS"] / count_table["total"]
            
            print(f"\n--- 统计 ---")
            print(count_table)

            # Plot
            ax = portionAxs[i]
            # 交叉表
            ct = pd.crosstab(df["cluster"], df["winner"])
            ct = ct.div(ct.sum(axis=1), axis=0)  # 按行归一化
            ct.plot(kind='bar', stacked=True, ax=ax, color=BAR_COLORS[0])
            ax.set_xlabel('Cluster')
            ax.set_ylabel('Proportion (%)')  # 或改为 'Percentage'
            ax.yaxis.set_major_formatter(PercentFormatter(1.0, symbol=None))  # 将0-1的比例格式化为百分比
            ax.legend(title='Winner')

            # Different cluster
            for j, cluster in enumerate(clusters):
                clusterData: pd.DataFrame = df[df['cluster'] == cluster]
                nTop = 10 # Fix number or portion?
                top = clusterData.nlargest(nTop, colAll)
                bottom = clusterData.nsmallest(nTop, colAll)

                print(f"\n簇 {cluster} - {name} ：")
                print(pd.concat([top[featureCols], bottom[featureCols]]).to_string())

                goodAvg = self.Xscaled[df.index.get_indexer(top.index), :].mean(axis=0)
                badAvg = self.Xscaled[df.index.get_indexer(bottom.index), :].mean(axis=0)

                self.__plotRadar(radarAxs[i*N+j], goodAvg, self.__INDICATOR_COLS, BAR_COLORS[0][1], "Good")
                self.__plotRadar(radarAxs[i*N+j], badAvg, self.__INDICATOR_COLS, BAR_COLORS[0][2], "Bad")

        portionPlots.legend(ncol=k)
        plt.plot(self.savePath, "clusting_portion", fig=portionPlots.fig)
        radarPlots.legend(ncol=k)
        plt.plot(self.savePath, "clusting_radar", fig=radarPlots.fig)
        

        return
    
    @staticmethod
    def __plotRadar(ax: plt.Axes, data: np.ndarray, categories: list, color: str, label: str) -> None:
        N = len(categories)
        angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
        data = np.concatenate((data, [data[0]]))  # 闭合
        angles += angles[:1]
        ax.plot(angles, data, 'o-', linewidth=2, color=color, label=label)
        ax.fill(angles, data, alpha=0.25, color=color)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.set_ylim(0, 1)  # 因为数据已归一化到 [0,1]

        return

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"

    a = clustering(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), INDICATOR, savePath=ANALY_RESULT)
    # a.compareK()
    a.run(k=4)
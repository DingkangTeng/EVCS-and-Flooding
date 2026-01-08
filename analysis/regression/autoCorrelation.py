import sys
import geopandas as gpd
import numpy as np
import seaborn as sns
from statsmodels.tools.tools import add_constant
from statsmodels.stats.outliers_influence import variance_inflation_factor
from matplotlib.patches import Circle

sys.path.append(".") # Set path to the roots

from _plot import plt

class autoCorrelation:
    __sltos__ = ["X_COL", "df", "Y_COL"]

    def __init__(
        self,
        df: gpd.pd.DataFrame | gpd.GeoDataFrame,
        xcol: list[str] | np.ndarray, ycol: list[str] | np.ndarray,
        checkCorelation: bool = True
    ) -> None:
        self.X_COL = xcol
        self.Y_COL = ycol
        self.df = df.dropna()
        
        if checkCorelation: self.__checkCollinearity()

        return
    
    def dropCorelation(self, dropCol: set[str], checkCorelation: bool = True) -> None:
        self.X_COL = [x for x in self.X_COL if x not in dropCol]
        if checkCorelation: self.__checkCollinearity()

        return

    def __checkCollinearity(self) -> None:
        X = self.df[self.X_COL]
        
        # 1. 相关系数矩阵
        print("\n" + "="*60)
        print("1. 变量相关系数矩阵")
        print("="*60)
        
        corr_matrix = X.corr()
        
        # 输出相关系数矩阵
        print("\n相关系数矩阵:")
        print(corr_matrix.round(4).to_string())
        
        # 2. 计算方差膨胀因子(VIF)
        print("\n" + "="*60)
        print("2. 方差膨胀因子(VIF)")
        print("="*60)
        
        X_const = add_constant(X)  # 添加常数项
        
        vif_data = gpd.pd.DataFrame()
        vif_data["feature"] = ["const"] + X.columns.tolist()
        vif_data["VIF"] = [variance_inflation_factor(X_const, i) 
                        for i in range(X_const.shape[1])]
        
        # 移除常数的VIF
        vif_data = vif_data[vif_data["feature"] != "const"]
        
        print("\nVIF值（VIF > 10表示严重的多重共线性）:")
        print(vif_data.round(2).to_string())
        
        # 标记VIF过高的变量
        high_vif = vif_data[vif_data["VIF"] > 10]
        if not high_vif.empty:
            print("\n警告：以下变量存在严重多重共线性（VIF > 10）:")
            for _, row in high_vif.iterrows():
                print(f"  {row['feature']}: VIF = {row['VIF']:.2f}")
        
        # 4. 可视化
        fig = plt.subplot("D", 2, 2, None, legend=False)
        axes = fig.axs
        
        # 4.1 相关系数热图
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt=".2f", 
                    cmap="coolwarm", center=0, ax=axes[0],
                    square=True, cbar_kws={"shrink": 0.8})
        axes[0].set_title("Variable correlation coefficient matrix")
        
        # 4.2 VIF条形图
        colors = ['red' if v > 10 else 'green' for v in vif_data["VIF"]]
        axes[1].barh(range(len(vif_data)), vif_data["VIF"], color=colors)
        axes[1].set_yticks(range(len(vif_data)))
        axes[1].set_yticklabels(vif_data["feature"])
        axes[1].axvline(x=10, color='r', linestyle='--', alpha=0.5)
        axes[1].set_xlabel("VIF")
        axes[1].set_title("VIF")
        
        # 4.3 相关系数网络图
        # 创建网络图
        n_vars = len(corr_matrix.columns)
        angles = np.linspace(0, 2*np.pi, n_vars, endpoint=False).tolist()
        radius = 1.0
        
        axes[2].set_aspect('equal')
        axes[2].axis('off')
        
        # 添加节点
        for i, (var, angle) in enumerate(zip(corr_matrix.columns, angles)):
            x = radius * np.cos(angle)
            y = radius * np.sin(angle)
            axes[2].add_patch(Circle((x, y), 0.05, color='blue', alpha=0.7))
            axes[2].text(x*1.15, y*1.15, var, ha='center', va='center', 
                        fontsize=9, rotation=angle*180/np.pi)
        
        # 添加边（只显示强相关）
        for i in range(n_vars):
            for j in range(i+1, n_vars):
                corr: float = abs(corr_matrix.iloc[i, j]) # type: ignore
                if corr > 0.5:
                    angle_i = angles[i]
                    angle_j = angles[j]
                    
                    x1 = radius * np.cos(angle_i)
                    y1 = radius * np.sin(angle_i)
                    x2 = radius * np.cos(angle_j)
                    y2 = radius * np.sin(angle_j)
                    
                    # 线宽表示相关强度
                    linewidth = 2 * corr
                    alpha = 0.3 + 0.7 * corr
                    
                    # 颜色表示正负相关
                    color = 'red' if corr_matrix.iloc[i, j] > 0 else 'blue' # type: ignore
                    
                    axes[2].plot([x1, x2], [y1, y2], color=color, 
                                linewidth=linewidth, alpha=alpha)
        
        axes[2].set_xlim(-1.5, 1.5)
        axes[2].set_ylim(-1.5, 1.5)
        axes[2].set_title("Circle (|r| > 0.5)")
        
        # 4.4 特征值比例图（检测多重共线性）
        X_normalized = (X - X.mean()) / X.std()
        corr_matrix_normalized = X_normalized.corr()
        eigenvalues = np.linalg.eigvals(corr_matrix_normalized)
        eigenvalues_sorted = np.sort(eigenvalues)[::-1]
        
        axes[3].plot(range(1, len(eigenvalues_sorted)+1), 
                    eigenvalues_sorted, 'bo-', linewidth=2)
        axes[3].axhline(y=0, color='r', linestyle='--', alpha=0.5)
        axes[3].set_xlabel("Eigenvalue sequence number")
        axes[3].set_ylabel("Eigenvalue")
        axes[3].set_title("Eigenvalue distribution")
        axes[3].grid(True, alpha=0.3)
        
        plt.plot()
        
        return

    def spatialAuto(self):
        """
        空间自相关分析，包括：
        1. 空间权重矩阵构建
        2. 全局莫兰指数（Moran's I）
        3. 局部莫兰指数（LISA聚类图）
        4. 空间滞后变量创建
        """
        import matplotlib.pyplot as plt
        from pysal.lib import weights
        import seaborn as sns
        from pysal.explore import esda
        
        print("\n" + "="*60)
        print("空间自相关分析")
        print("="*60)
        
        # 1. 构建空间权重矩阵
        print("\n1. 构建空间权重矩阵...")
        
        # 使用KNN权重（适用于不规则分布）
        coords = list(zip(self.df.geometry.centroid.x, self.df.geometry.centroid.y))
        
        # 计算合适的K值（约为总样本数的平方根）
        k = max(1, min(8, int(np.sqrt(self.df.shape[0]))))
        
        # KNN权重矩阵
        knn = weights.KNN.from_array(coords, k=k)
        knn.transform = 'r'  # 行标准化
        
        print(f"  使用KNN权重，k={k}")
        
        # 2. 全局莫兰指数检验
        print("\n2. 全局空间自相关检验（Moran's I）:")
        
        moran_results = {}
        
        # 检验解释变量
        for col in self.X_COL:
            moran = esda.Moran(self.df[col], knn)
            moran_results[f"X_{col}"] = {
                'I': moran.I,
                'p_value': moran.p_norm,
                'z_score': moran.z_norm
            }
        
        # 检验目标变量（如果提供）
        for col in self.Y_COL:
            moran = esda.Moran(self.df[col], knn)
            moran_results[f"Y_{col}"] = {
                'I': moran.I,
                'p_value': moran.p_norm,
                'z_score': moran.z_norm
            }
        
        # 创建结果DataFrame
        moran_df = gpd.pd.DataFrame(moran_results).T
        moran_df['significant'] = moran_df['p_value'] < 0.05
        moran_df['interpretation'] = moran_df.apply(
            lambda row: "显著正相关" if row['I'] > 0 and row['significant'] 
            else "显著负相关" if row['I'] < 0 and row['significant'] 
            else "不显著", axis=1
        )
        
        print("\n全局Moran's I检验结果:")
        print(moran_df.round(4).to_string())
        
        # 3. 可视化全局Moran's I结果
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 3.1 Moran's I条形图
        colors = ['green' if i > 0 else 'red' for i in moran_df['I']]
        axes[0, 0].barh(range(len(moran_df)), moran_df['I'], color=colors)
        axes[0, 0].set_yticks(range(len(moran_df)))
        axes[0, 0].set_yticklabels(moran_df.index)
        axes[0, 0].axvline(x=0, color='k', linestyle='-', alpha=0.3)
        axes[0, 0].set_xlabel("Moran's I")
        axes[0, 0].set_title("全局空间自相关 (Moran's I)")
        
        # 添加显著标记
        for i, (idx, row) in enumerate(moran_df.iterrows()):
            if row['significant']:
                axes[0, 0].text(row['I'], i, '*', ha='left' if row['I'] > 0 else 'right', 
                            va='center', fontsize=12, color='blue')
        
        # 3.2 P值热图
        p_values = moran_df[['p_value']].copy()
        p_values['-log10(p)'] = -np.log10(p_values['p_value'])
        
        sns.heatmap(np.asarray(p_values['-log10(p)'].values).reshape(-1, 1), 
                    annot=True, fmt=".2f", cmap="YlOrRd", 
                    ax=axes[0, 1], cbar_kws={'label': '-log10(p-value)'})
        axes[0, 1].set_yticklabels(p_values.index)
        axes[0, 1].set_title("显著性水平 (-log10(p-value))")
        axes[0, 1].axhline(y=0, color='k', linewidth=2)
        axes[0, 1].axhline(y=len(p_values), color='k', linewidth=2)
        
        # 4. 局部莫兰指数（LISA聚类）
        print("\n3. 局部空间自相关分析（LISA聚类）...")
        
        # 选择一个变量进行LISA分析（例如第一个目标变量或第一个解释变量）
        var_for_lisa = self.Y_COL[0]
        
        lisa = esda.Moran_Local(self.df[var_for_lisa], knn)
        
        # 创建LISA聚类图
        from matplotlib import colors as mcolors
        from matplotlib.patches import Patch
        
        # 定义LISA聚类类型
        sig = lisa.p_sim < 0.05
        lisa_clusters = lisa.q * sig  # 0=不显著，1=HH，2=LH，3=LL，4=HL
        
        # 创建颜色映射
        cluster_colors = {
            0: 'lightgrey',  # 不显著
            1: 'red',        # HH（高-高）
            2: 'lightblue',  # LH（低-高）
            3: 'blue',       # LL（低-低）
            4: 'pink'        # HL（高-低）
        }
        
        cluster_labels = {
            0: '不显著',
            1: '高-高聚类',
            2: '低-高聚类',
            3: '低-低聚类',
            4: '高-低聚类'
        }
        
        # 绘制空间分布图
        gdf_copy = self.df.copy()
        gdf_copy['lisa_cluster'] = lisa_clusters
        gdf_copy['cluster_color'] = gdf_copy['lisa_cluster'].map(cluster_colors)
        
        base = gdf_copy.plot(color=gdf_copy['cluster_color'], 
                            edgecolor='black', linewidth=0.5, 
                            ax=axes[1, 0], figsize=(10, 8))
        
        # 创建图例
        legend_elements = []
        for cluster_id, label in cluster_labels.items():
            if cluster_id in gdf_copy['lisa_cluster'].unique():
                legend_elements.append(
                    Patch(facecolor=cluster_colors[cluster_id], 
                        edgecolor='black', label=label)
                )
        
        axes[1, 0].legend(handles=legend_elements, loc='upper left', 
                        bbox_to_anchor=(1.05, 1))
        axes[1, 0].set_title(f"{var_for_lisa} 的LISA聚类图")
        axes[1, 0].axis('off')
        
        # 5. 空间滞后变量创建
        print("\n4. 创建空间滞后变量...")
        
        # 创建空间滞后变量（邻居的加权平均）
        spatial_lag_df = gpd.pd.DataFrame(index=self.df.index)
        
        for col in self.X_COL:
            lag_var = weights.lag_spatial(knn, self.df[col].values)
            spatial_lag_df[f"lag_{col}"] = lag_var
        
        # 6. Moran散点图
        axes[1, 1].scatter(self.df[var_for_lisa], weights.lag_spatial(knn, self.df[var_for_lisa]), 
                        alpha=0.6, edgecolors='w', linewidth=0.5)
        
        # 添加象限线
        x_mean = self.df[var_for_lisa].mean()
        y_mean = weights.lag_spatial(knn, self.df[var_for_lisa]).mean()
        
        axes[1, 1].axhline(y=y_mean, color='r', linestyle='--', alpha=0.5)
        axes[1, 1].axvline(x=x_mean, color='r', linestyle='--', alpha=0.5)
        
        # 添加象限标签
        axes[1, 1].text(0.95, 0.95, 'HH', transform=axes[1, 1].transAxes, 
                    fontsize=12, ha='right', va='top', color='red')
        axes[1, 1].text(0.05, 0.95, 'LH', transform=axes[1, 1].transAxes, 
                    fontsize=12, ha='left', va='top', color='blue')
        axes[1, 1].text(0.05, 0.05, 'LL', transform=axes[1, 1].transAxes, 
                    fontsize=12, ha='left', va='bottom', color='blue')
        axes[1, 1].text(0.95, 0.05, 'HL', transform=axes[1, 1].transAxes, 
                    fontsize=12, ha='right', va='bottom', color='red')
        
        axes[1, 1].set_xlabel(f"{var_for_lisa}")
        axes[1, 1].set_ylabel(f"空间滞后 {var_for_lisa}")
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        plt.close()
        
        # 保存空间滞后变量
        # spatial_lag_df.to_csv(os.path.join(output_dir, "spatial_lag_variables.csv"))
        
        # 保存LISA结果
        lisa_results = gpd.pd.DataFrame({
            'variable': [var_for_lisa] * self.df.shape[0],
            'lisa_statistic': lisa.Is,
            'p_value': lisa.p_sim,
            'cluster': lisa_clusters,
            'cluster_label': [cluster_labels.get(c, '未知') for c in lisa_clusters]
        }, index=self.df.index)
        
        # lisa_results.to_csv(os.path.join(output_dir, "lisa_results.csv"))
        
        return {
            'knn_weights': knn,
            'moran_results': moran_df,
            'spatial_lag_variables': spatial_lag_df,
            'lisa_results': lisa_results
        }
import sys, os
import geopandas as gpd
import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from statsmodels.regression.linear_model import RegressionResultsWrapper

sys.path.append(".") # Set path to the roots

from analysis.regression import autoCorrelation
from analysis.__statisticalDiff import significanceStars
from _plot import plt, BAR_COLORS, NOTE_SIZE

STAND_NAME = {
    "EVCScoverage": "EVCS coverage",
    "folldingCoverage": "Flooding coverage",
    "EVCSChange": "EVCS change"
}

class linearRegressor(autoCorrelation):
    
    def linearRegression(self, figsize: str = "WN32", savePath: str = "") -> None:
        X = self.df[self.X_COL]
        y = self.df[self.Y_COL].to_numpy().flatten()

        # Add constant term for intercept
        Xsm = sm.add_constant(X)
        model = sm.OLS(y, Xsm)
        results = model.fit()
        # yPred = results.predict(Xsm)

        # Calculate standardized coefficients for variable importance
        scaler = StandardScaler()
        XScaled = scaler.fit_transform(X)
        yScaled = (y - y.mean()) / y.std()
        model_scaled = LinearRegression()
        model_scaled.fit(XScaled, yScaled)
        
        multiplot = plt.subplot(figsize, 1, 3, widthRatios=[1, 1, 1], legend=False)
        axs = multiplot.axs

        # Regression equation and statistics summary
        ## Equation
        equationText = f"Regression Equation:\n"
        equationText += f"Y = {results.params["const"]:.4f} "
        for var in self.X_COL:
            equationText += f"+ {results.params[var]:.4f} × {var} "

        ## Model evaluation text
        statsText = ""
        statsText += f"R² = {results.rsquared:.4f}\n"
        statsText += f"Adjusted R² = {results.rsquared_adj:.4f}\n"
        statsText += f"F-statistic = {results.fvalue:.2f}\n"
        statsText += f"F-test p-value = {results.f_pvalue:.4f}\n"
        statsText += f"Sample Size = {int(results.nobs)}"

        ## Variable significance text
        coefText = "\nVariable Significance:"
        for var in self.X_COL:
            p = results.pvalues[var]
            coefText += f"\n{var}: β={results.params[var]:.4f}, p={p:.4f} ({significanceStars.sign(p)})"

        ## Add text
        print(equationText + "\n" + coefText + "\nModel Evaluation:\n" + statsText)

        # variable importance bar chart
        ax0 = axs[0]
        coefValues = model_scaled.coef_
        if coefValues.ndim > 1:
            coefValues = coefValues.flatten()
        coefAbs = np.abs(coefValues)

        ## Bars
        colors = BAR_COLORS[0][:len(self.X_COL)]
        bars = ax0.bar(self.X_COL, coefAbs, color=colors, alpha=0.8, edgecolor="black", linewidth=1.2)

        ## Add value labels on bars
        for i, (bar, coef) in enumerate(zip(bars, coefValues)):
            height = bar.get_height()
            ax0.text(
                bar.get_x() + bar.get_width()/2., height + 0.01,
                f"{coef:.4f} ({significanceStars.sign(results.pvalues[self.X_COL[i]])})",
                ha="center", va="bottom", fontsize=NOTE_SIZE, fontweight="bold"
            )

        ax0.set_ylabel("Absolute standardized coefficient")
        ax0.set_xticks(range(len(self.X_COL)))
        ax0.set_xticklabels([STAND_NAME[x] for x in self.X_COL])

        self.partialDependece(results, self.X_COL[0], self.X_COL[1], axs[1], "blue")
        self.partialDependece(results, self.X_COL[1], self.X_COL[0], axs[2], "red")

        # ## Scatter plot with residuals as color
        # scatter = ax1.scatter(
        #     yPred, y, c=residuals,
        #     cmap="coolwarm_r", alpha=0.7, edgecolors='k', linewidth=0.5, s=60
        # )

        # # Add y=x reference line
        # minVal = min(y.min(), yPred.min())
        # maxVal = max(y.max(), yPred.max())
        # ax1.plot([minVal, maxVal], [minVal, maxVal], 'k--', linewidth=1.5, label="y=x Reference")

        # # Add colorbar and labels and r2
        # cbar = plt.plt.colorbar(scatter, ax=ax1)
        # cbar.set_label("Residuals")
        # yColsName = " & ".join([STAND_NAME[y] for y in self.Y_COL])
        # ax1.set_xlabel("Predicted {}".format(yColsName))
        # ax1.set_ylabel("Actual {}".format(yColsName))
        # ax1.text(
        #     0.05, 0.95,
        #     statsText,
        #     transform=ax1.transAxes,
        #     fontsize=NOTE_SIZE, verticalalignment="top",
        #     bbox=dict(boxstyle="round", facecolor="white", alpha=0.8)
        # )

        plt.plot(savePath, "LinearRegression_Summary.jpg")

        # Print regression result
        print("=" * 70)
        print("Multiple Linear Regression Results:")
        print("=" * 70)
        print(results.summary())

        print("\n" + "=" * 70)
        print("Results Explanation:")
        print("=" * 70)
        i = 1
        for var in self.X_COL:
            print(f"{i}. For every 1% increase in {var}, EVCS change rate increases by {results.params[var]:.4f}%")
            i += 1
        print(f"{i}. The model explains {results.rsquared*100:.1f}% of the variation in EVCS change rate.")
        print(
            f"{i+1}. The standardized coefficients show that \
            {STAND_NAME[self.X_COL[0]] if abs(model_scaled.coef_[0]) > abs(model_scaled.coef_[1]) else STAND_NAME[self.X_COL[1]]} \
            has a greater impact on change rate."
        )

        return
    
    # def singleChange(self, results: RegressionResultsWrapper, var1: str, var2: str, ax: plt.Axes) -> None:
    #     # 生成 var1 的取值范围
    #     var_range = np.linspace(self.df[var1].min(), self.df[var1].max(), 100)
    #     fixed_val = self.df[var2].mean()          # 固定 var2 为均值
        
    #     # 构建预测数据，显式包含常数项，并确保列顺序与模型一致（const, var1, var2）
    #     n = len(var_range)
    #     X_pred = pd.DataFrame({
    #         'const': np.ones(n),          # 常数项列
    #         var1: var_range,
    #         var2: np.full(n, fixed_val)
    #     })
        
    #     # 预测
    #     y_pred = results.predict(X_pred)
        
    #     # 绘图
    #     ax.plot(var_range, y_pred, 'b-', linewidth=2, label=f'{var2} = {fixed_val:.2f}')
    #     ax.scatter(self.df[var1], self.df[self.Y_COL[0]],  # Y_COL 是列表，取第一个元素
    #             color='gray', alpha=0.5, s=30, label='Actual data')
    #     ax.set_xlabel(STAND_NAME.get(var1, var1))
    #     ax.set_ylabel(f'Predicted {self.Y_COL[0]}')
    #     ax.legend()
    #     ax.grid(True, linestyle='--', alpha=0.6)

    #     return

    def partialDependece(self, results: RegressionResultsWrapper, xCol: str, fixCol: str, ax: plt.Axes, color: str) -> None:
        x1 = np.linspace(self.df[xCol].min(), self.df[xCol].max(), 50)
        # Original OSL model
        const = results.params["const"]
        b1 = results.params[xCol]
        b2 = results.params[fixCol]
        yPred = const + b1 * x1 + b2 * self.df[fixCol].mean()
    
        vals = np.linspace(self.df[xCol].min(), self.df[xCol].max(), 50)

        # Real data points
        ax.scatter(self.df[xCol], self.df[self.Y_COL], color="gray", alpha=0.5, s=10, label="Actual data")
        # Partial dependence line
        ax.plot(vals, yPred, 'b-', linewidth=2.5, label="Partial dependence", color=color)
        ax.fill_between(vals, yPred, alpha=0.2, color=color)
        ax.set_xlabel(STAND_NAME[xCol])
        ax.set_ylabel(f"Predicted {STAND_NAME[self.Y_COL[0]]}")
        
        return

# Debug
if __name__ == "__main__":
    ANALY_RESULT_ROOT = r"C:\\0_PolyU\\test"
    INDICATOR = os.path.join(ANALY_RESULT_ROOT, "indicator.gpkg")

    EVCSRegCol = [
        "EVCScoverage", "folldingCoverage"
    ]
    df = gpd.read_file(INDICATOR, layer="city")[EVCSRegCol + ["city", "EVCSChange", "geometry"]]
    a = linearRegressor(df, EVCSRegCol, ["EVCSChange"], checkCorelation=False)
    a.linearRegression(savePath=ANALY_RESULT_ROOT)
    # a.spatialAuto()
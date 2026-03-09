import sys, os
import geopandas as gpd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

sys.path.append(".") # Set path to the roots

from analysis.regression import autoCorrelation
from analysis.__statisticalDiff import significanceStars
from _plot import plt, BAR_COLORS, NOTE_SIZE

STAND_NAME = {
    "EVCScoverage": "EVCS Coverage",
    "folldingCoverage": "Flooding Coverage",
}

class linearRegressor(autoCorrelation):
    
    def linearRegression(self, figsize: str = "WN31", savePath: str = "") -> None:
        X = self.df[self.X_COL]
        y = self.df[self.Y_COL].to_numpy().flatten()

        # Add constant term for intercept
        Xsm = sm.add_constant(X)
        model = sm.OLS(y, Xsm)
        results = model.fit()
        yPred = results.predict(Xsm)

        # Calculate standardized coefficients for variable importance
        scaler = StandardScaler()
        XScaled = scaler.fit_transform(X)
        yScaled = (y - y.mean()) / y.std()
        model_scaled = LinearRegression()
        model_scaled.fit(XScaled, yScaled)
        
        multiplot = plt.subplot(figsize, 1, 2, widthRatios=[1, 1], legend=False)
        axs = multiplot.axs

        # Regression equation and statistics summary
        ## Equation
        equationText = f"Regression Equation:\n"
        equationText += f"Y = {results.params["const"]:.4f} "
        for var in self.X_COL:
            equationText += f"+ {results.params[var]:.4f} × {var} "

        ## Model evaluation text
        statsText = f"Model Evaluation:\n"
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
        print(equationText + "\n" + coefText + "\n" + statsText)

        # variable importance bar chart
        ax3 = axs[0]
        coefValues = model_scaled.coef_
        if coefValues.ndim > 1:
            coefValues = coefValues.flatten()
        coefAbs = np.abs(coefValues)

        ## Bars
        colors = BAR_COLORS[0][:len(self.X_COL)]
        bars = ax3.bar(self.X_COL, coefAbs, color=colors, alpha=0.8, edgecolor="black", linewidth=1.2)

        ## Add value labels on bars
        for i, (bar, coef) in enumerate(zip(bars, coefValues)):
            height = bar.get_height()
            ax3.text(
                bar.get_x() + bar.get_width()/2., height + 0.01,
                f"{coef:.4f} ({significanceStars.sign(results.pvalues[self.X_COL[i]])})",
                ha="center", va="bottom", fontsize=NOTE_SIZE, fontweight="bold"
            )

        ax3.set_ylabel("Absolute standardized coefficient")

        # Residual analysis scatter plot
        ax4 = axs[1]
        residuals = y - yPred

        ## Scatter plot with residuals as color
        scatter = ax4.scatter(
            yPred, y, c=residuals,
            cmap="coolwarm_r", alpha=0.7, edgecolors='k', linewidth=0.5, s=60
        )

        # Add y=x reference line
        minVal = min(y.min(), yPred.min())
        maxVal = max(y.max(), yPred.max())
        ax4.plot([minVal, maxVal], [minVal, maxVal], 'k--', linewidth=1.5, label="y=x Reference")

        # Add colorbar and labels and r2
        cbar = plt.plt.colorbar(scatter, ax=ax4)
        cbar.set_label("Residuals")
        ax4.set_xlabel("Predicted EVCS change")
        ax4.set_ylabel("Actual EVCS change")
        ax4.text(
            0.05, 0.95,
            f'R² = {results.rsquared:.4f}',
            transform=ax4.transAxes,
            fontsize=NOTE_SIZE, verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8)
        )

        plt.plot(savePath, "LinearRegression_Summary.jpg")

        # 3. 打印详细的回归结果
        print("=" * 70)
        print("Multiple Linear Regression Results:")
        print("=" * 70)
        print(results.summary())

        # 4. 打印简单解释
        print("\n" + "=" * 70)
        print("Resutlt Explanation:")
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
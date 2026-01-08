import pandas as pd
import numpy as np
import scipy.stats as stats

from ..__setting import significanceStars

# Wilcoxon signed-rank test
def Wilcoxon(
    compareCol: list[str] | np.ndarray, df: pd.DataFrame, yName: str = "value",
    savePath: str = "", show: bool = False
) -> pd.DataFrame:
    if savePath == "" or show: print(f"\nWilcoxon signed-rank test on {yName}:")

    wilcoxon = []
    for i, group1 in enumerate(compareCol):
        for group2 in compareCol[i+1:]:
            subdf = df[[group1, group2]].dropna()
            stat, p = stats.wilcoxon(subdf[group1], subdf[group2])
            p = float(np.asarray(p).item())
            
            # Calculate the effect quantity
            n = subdf[group1].shape[0]
            z = stats.norm.ppf(p/2) if p < 1 else 0
            r = abs(z) / np.sqrt(n)
            
            wilcoxon.append({
                ".y.": yName,
                "group1": group1,
                "group2": group2,
                "effsize": round(r, 4),
                "n1": n,
                "n2": n,                
                "magnitude": "negligible" if r < 0.1 else "small" if r < 0.3 else "moderate" if r < 0.5 else "large",
                "w": stat,
                "p": round(p, 4),
                "significance": significanceStars.sign(p)
            })

    wilcoxon = pd.DataFrame(wilcoxon)

    if savePath != "":
        wilcoxon.to_csv(savePath, index=False, encoding="utf-8")
        with open(savePath, 'a', encoding="utf-8") as f:
            f.write(significanceStars.SIGN_CODES)
    
    elif show:
        print(wilcoxon, significanceStars.SIGN_CODES)

    return wilcoxon
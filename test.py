import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns

from analysis.__statisticalDiff.nonpara.Wilcoxon import Wilcoxon
# 示例数据结构
data = {
    'city': ['City1', 'City2', 'City3', 'City4', 'City5', 'City6', 'City7', 'City8'],
    'male_access': [15.2, 18.5, 12.3, 22.1, 14.8, 19.7, 16.4, 13.9],
    'female_access': [13.8, 16.2, 10.9, 19.8, 13.1, 17.5, 14.9, 12.3],
    'total_access': [14.5, 17.4, 11.6, 21.0, 14.0, 18.6, 15.7, 13.1]
}

df = pd.DataFrame(data)

Wilcoxon(["male_access", 'female_access','total_access'], df)
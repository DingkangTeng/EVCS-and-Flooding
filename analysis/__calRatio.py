from numpy import ndarray, uint16
from pandas import DataFrame
from typing import overload, Literal

@overload
def calRatio(
    df: DataFrame, colsBefore: list[str], colsAfter: list[str],
    seperateZero: Literal[True], accOrEquity: str = ""
) -> tuple[DataFrame, ndarray, ndarray, ndarray]: ...

@overload
def calRatio(
    df: DataFrame, colsBefore: list[str], colsAfter: list[str],
    seperateZero: Literal[False] = False, accOrEquity: str = ""
) -> tuple[DataFrame, ndarray]: ...

def calRatio(
    df: DataFrame, colsBefore: list[str], colsAfter: list[str],
    seperateZero: bool = False, accOrEquity: str = ""
) -> tuple[DataFrame, ndarray] | tuple[DataFrame, ndarray, ndarray, ndarray]:
    resultCols = ndarray([len(colsBefore)], dtype=object)
    zeroCounts = ndarray([len(colsBefore)], dtype=uint16)
    nonZeroCounts = ndarray([len(colsBefore)], dtype=uint16)

    for i, a in enumerate(colsBefore):
        col = "{}_changeresultCols".format(a)
        resultCols[i] = col
        df[col] = (df[colsAfter[i]] / df[a] - 1) * 100
        allRecord = df[df[col].notna()]
        affected = allRecord[allRecord[col] != 0]
        print(
            f"In {a}, {affected.shape[0]} nodes/region ({affected.shape[0] / allRecord.shape[0] * 100:.2f}%) are affected by flooding."
        )
        if accOrEquity == "equity":
            decrease = df[df[col] > 0].shape[0]
            print(
                f"In {a}, {decrease} nodes/region ({decrease / affected.shape[0] * 100:.2f}%) in the affecred nodes have a decrease in equity."
            )

        if seperateZero:
            # Statistic 0 and non-0
            zeroCount = (df[col] == 0).sum()
            zeroCounts[i] = zeroCount
            nonZeroCounts[i] = allRecord.shape[0] - zeroCount
        
    if seperateZero:
        return df, resultCols, zeroCounts, nonZeroCounts
    else:
        return df, resultCols
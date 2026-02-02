import pandas as pd
import numpy as np

def transform(df: pd.DataFrame, metric: list) -> pd.DataFrame:
    """
    Replicates the PySpark logic in pandas.

    Parameters
    ----------
    df : pd.DataFrame
        Source DataFrame, equivalent to spark_df.
        Must contain columns: 'metric', 'limits', 'column_name',
        and the value columns ['result', 'previous_result', 'difference'].
    metric : list
        Python list of metrics to keep (used in .isin()).

    Returns
    -------
    pd.DataFrame
        Pivoted result (index: limits, column_name; columns from mapped 'y').
    """
    value_cols = ["result", "previous_result", "difference"]
    id_vars = [c for c in df.columns if c not in value_cols]

    # 1) Unpivot (wide → long), equivalent to Spark inline of array<struct<y,z>>
    df_piv = df.melt(
        id_vars=id_vars,
        value_vars=value_cols,
        var_name="y",      # 'result' / 'previous_result' / 'difference'
        value_name="z"     # value taken from the respective column
    )

    # 2) Map y based on metric
    conds = [
        (df_piv["metric"].eq("distribution") & df_piv["y"].eq("result")),
        (df_piv["metric"].eq("distribution") & df_piv["y"].eq("previous_result")),
        (df_piv["metric"].eq("distribution") & df_piv["y"].eq("difference")),
        (df_piv["metric"].eq("psi")          & df_piv["y"].eq("result")),
        (df_piv["metric"].eq("large_number") & df_piv["y"].eq("result")),
        (df_piv["metric"].eq("large_number") & df_piv["y"].eq("previous_result")),
    ]
    choices = ["pct_is", "pct_was", "variation", "psi", "count_is", "count_was"]
    df_piv["y"] = np.select(conds, choices, default="except")

    # 3) Filter to desired metric values and select the same columns as Spark
    df_result = df_piv[df_piv["metric"].isin(metric)][["limits", "metric", "column_name", "y", "z"]]

    # 4) Pivot (groupby + pivot + first aggregation)
    final = (
        df_result
        .pivot_table(
            index=["limits", "column_name"],
            columns="y",
            values="z",
            aggfunc="first"   # mirrors F.first("z")
        )
        .reset_index()
    )
    final.columns.name = None  # drop the pivoted column index name for cleanliness
    return final

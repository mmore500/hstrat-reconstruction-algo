import pandas as pd
import polars as pl
from tqdm import tqdm


def agg_characters_extrema(characters_df: pl.DataFrame) -> pd.DataFrame:

    dfx_list = []
    for dfp in tqdm(characters_df.partition_by("replicate")):
        dfx_list.append(
            dfp.group_by(
                [
                    "group",
                    "replicate",
                    "population size",
                    "available beneficial mutations",
                ],
            ).agg(
                pl.col("fitness")
                .min()
                .alias(
                    "min fitness",
                ),
                pl.col("fitness")
                .max()
                .alias(
                    "max fitness",
                ),
                pl.col("fitness")
                .mean()
                .alias(
                    "mean fitness",
                ),
                pl.col("fitness")
                .median()
                .alias(
                    "median fitness",
                ),
                pl.col("accrued beneficial")
                .min()
                .alias(
                    "min accrued beneficial",
                ),
                pl.col("accrued beneficial")
                .max()
                .alias(
                    "max accrued beneficial",
                ),
                pl.col("accrued beneficial")
                .mean()
                .alias(
                    "mean accrued beneficial",
                ),
                pl.col("accrued beneficial")
                .median()
                .alias(
                    "median accrued beneficial",
                ),
                pl.col("accrued deleterious")
                .min()
                .alias(
                    "min accrued deleterious",
                ),
                pl.col("accrued deleterious")
                .max()
                .alias(
                    "max accrued deleterious",
                ),
                pl.col("accrued deleterious")
                .mean()
                .alias(
                    "mean accrued deleterious",
                ),
                pl.col("accrued deleterious")
                .median()
                .alias(
                    "median accrued deleterious",
                ),
                pl.col("accrued hypermutator")
                .min()
                .alias(
                    "min accrued hypermutator",
                ),
                pl.col("accrued hypermutator")
                .max()
                .alias(
                    "max accrued hypermutator",
                ),
                pl.col("accrued hypermutator")
                .mean()
                .alias(
                    "mean accrued hypermutator",
                ),
                pl.col("accrued hypermutator")
                .median()
                .alias(
                    "median accrued hypermutator",
                ),
                pl.col("founder")
                .n_unique()
                .alias(
                    "founder nunique",
                ),
            ),
        )

    dfxs = pl.concat(dfx_list)

    return dfxs.to_pandas()

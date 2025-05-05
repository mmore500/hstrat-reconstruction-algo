import pandas as pd
import polars as pl
from tqdm import tqdm


def calc_fixprobs_from_traits(traits_df: pd.DataFrame) -> pd.DataFrame:

    traits_df["available beneficial mutations"] = (
        traits_df["genomeFlavor"]
        .str.extract(r"(\d+)", expand=False)
        .fillna(1)
        .astype("int8")
    )
    traits_df.drop(columns=["genomeFlavor"], inplace=True)

    df = pl.from_pandas(traits_df)

    df = df.with_columns(
        pl.col("replicate").hash(),
    )

    df = df.select(
        [
            "available beneficial mutations",
            "row",
            "col",
            "nRowSubgrid",
            "nColSubgrid",
            "trait count",
            "tilePopSize",
            "trait value",
            "replicate",
        ],
    )

    df = df.with_columns(
        (pl.col("row").cast(pl.UInt64) // pl.col("nRowSubgrid"))
        .alias("rowGroup")
        .cast(pl.UInt16),
        (pl.col("col").cast(pl.UInt64) // pl.col("nColSubgrid"))
        .alias("colGroup")
        .cast(pl.UInt16),
    )

    df = df.with_columns(
        (
            pl.when(
                pl.col("nColSubgrid") == 0,
            )
            .then(
                pl.col("col").max() + 1,
            )
            .otherwise(
                pl.col("nColSubgrid"),
            )
            .cast(pl.UInt64)
            * pl.when(
                pl.col("nRowSubgrid") == 0,
            )
            .then(
                pl.col("row").max() + 1,
            )
            .otherwise(
                pl.col("nRowSubgrid"),
            )
            .cast(pl.UInt64)
            * pl.col("tilePopSize").cast(pl.UInt64)
        ).alias("population size"),
        (pl.col("trait count") / pl.col("tilePopSize")).alias(
            "fixation probability"
        ),
    ).drop(
        "nColSubgrid",
        "nRowSubgrid",
        "tilePopSize",
        "trait count",
        "col",
        "row",
    )

    df = df.with_columns(
        (
            pl.col("colGroup").cast(pl.UInt64)
            + (pl.col("colGroup").max() + 1).cast(pl.UInt64)
            * pl.col("rowGroup").cast(pl.UInt64)
        )
        .alias("group")
        .cast(pl.UInt32),
    ).drop("rowGroup", "colGroup")

    df = df.select(
        [
            "group",
            "trait value",
            "replicate",
            "population size",
            "available beneficial mutations",
            "fixation probability",
        ],
    )

    dfx_list = []
    for dfp in tqdm(df.partition_by("replicate")):
        dfx_list.append(
            dfp.group_by(
                [
                    "group",
                    "trait value",
                    "replicate",
                    "population size",
                    "available beneficial mutations",
                ],
            ).agg(
                pl.col("fixation probability").mean(),
            ),
        )

    dfxs = pl.concat(dfx_list)
    dfxs = dfxs.with_columns(
        pl.col("trait value")
        .replace_strict(
            {0: "normomutator", 1: "hypermutator"},
        )
        .cast(pl.Categorical)
        .alias("genotype")
    )

    return dfxs.to_pandas()

import pandas as pd
import polars as pl


def calc_characters_from_genomes_cupy(
    genomes_df: pd.DataFrame,
) -> pl.DataFrame:

    genomes_df["available beneficial mutations"] = (
        genomes_df["genomeFlavor"]
        .str.extract("(\d+)", expand=False)
        .fillna(1)
        .astype("int8")
    )
    genomes_df.drop(columns=["genomeFlavor"], inplace=True)

    genomes_df["accrued beneficial"] = (
        genomes_df["bitfield"].str.slice(0, 2).apply(int, base=16)
    )
    genomes_df["accrued deleterious"] = (
        genomes_df["bitfield"].str.slice(2, 4).apply(int, base=16)
    )
    genomes_df["accrued hypermutator"] = (
        genomes_df["bitfield"].str.slice(4, 6).apply(int, base=16)
    )
    genomes_df["founder"] = (
        genomes_df["bitfield"].str.slice(6, 8).apply(int, base=16)
    )
    genomes_df.drop(columns=["bitfield"], inplace=True)

    df = pl.from_pandas(genomes_df)

    df = df.with_columns(
        pl.col("replicate").hash(),
    )

    df = df.select(
        [
            "available beneficial mutations",
            "fitness",
            "accrued beneficial",
            "accrued deleterious",
            "accrued hypermutator",
            "founder",
            "row",
            "col",
            "nRowSubgrid",
            "nColSubgrid",
            "tilePopSize",
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
    ).drop("nColSubgrid", "nRowSubgrid", "tilePopSize", "col", "row")

    df = df.with_columns(
        (
            pl.col("colGroup").cast(pl.UInt64)
            + (pl.col("colGroup").max() + 1).cast(pl.UInt64)
            * pl.col("rowGroup").cast(pl.UInt64)
        )
        .alias("group")
        .cast(pl.UInt32),
    ).drop("rowGroup", "colGroup")

    return df

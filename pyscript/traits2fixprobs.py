#!/usr/bin/env python

import sys

import pandas as pd

from pylib.munge._calc_fixprobs_from_traits import calc_fixprobs_from_traits

if __name__ == "__main__":
    __, target_path = sys.argv

    print(f"reading {target_path}")
    df = pd.read_parquet(target_path)
    print(f"read {len(df)} rows")
    df = calc_fixprobs_from_traits(df)
    out_path = target_path.replace("a=traits", "a=fixprobs")
    print(f"writing {out_path}")
    df.to_parquet(target_path.replace("a=traits", "a=fixprobs"))

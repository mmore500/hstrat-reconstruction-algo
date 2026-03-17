#!/bin/bash
#SBATCH --job-name=phylo_reprocess
#SBATCH --time=04:00:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --account=ecode
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --array=0-15

SEED=${SLURM_ARRAY_TASK_ID}

# 1. Copy the data to the node's local /tmp directory
singularity exec docker://rclone/rclone rclone copyto /mnt/home/mmore500/scratch/hstrat-reconstruction-algo/2026-03-12-reprocess-billiontip/latest/._0/a=phylo+ext=.pqt /tmp/phylo-${SEED}.pqt --progress --multi-thread-streams 4

# 2. Run the Polars processing pipeline
export POLARS_ENGINE_AFFINITY="streaming"
ls /tmp/phylo-${SEED}.pqt | singularity exec docker://ghcr.io/mmore500/phyloframe:v0.6.0 python3 -m phyloframe.legacy._alifestd_pipe_unary_ops_polars \
  --op 'lambda df: pfl.alifestd_mark_sample_tips_lineage_stratified_polars(df, mark_as="is_lineage", criterion_delta="hstrat_rank", criterion_target="layer", criterion_stratify="layer", n_tips_per_stratum=1, seed='"${SEED}"')' \
  --op 'lambda df: pfl.alifestd_mark_sample_tips_polars(df, n_sample=50_000, mark_as="is_foliage", seed='"${SEED}"')' \
  --op 'lambda df: df.with_columns(extant=pl.col("is_foliage") | pl.col("is_lineage"))' \
  --op 'pfl.alifestd_prune_extinct_lineages_polars' \
  /tmp/dsamp-${SEED}.pqt --eager-write

# 3. Copy the result back to your home directory
cp /tmp/dsamp-${SEED}.pqt ~/2026-03-12-reprocess-billiontip-0-sfl-dsamp-seed${SEED}.pqt

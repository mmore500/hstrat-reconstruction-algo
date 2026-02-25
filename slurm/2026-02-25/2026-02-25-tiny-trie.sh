#!/bin/bash -login

set -euo pipefail

cd "$(dirname "$0")"

################################################################################
# CLI flag handling
################################################################################
show_help() {
    cat << 'HELPEOF'
Usage: 2026-02-24-tiny-trie.sh [OPTIONS]

Workflow to build a tiny trie from downsampled fossils.

Options:
  --submit               Submit the SLURM workflow (or run locally if no
                         sbatch is available).
  --archive-latest       Archive the latest workflow output to
                         ${HOME}/archive/ via rclone.
  --archive-latest-check Print recursive du -h of the archive directory.
  --archive-latest-purge Purge the archive directory via rclone purge
                         (must be run before re-archiving).
  --check-result         Print recursive du -h of the latest result
                         directory (full absolute real paths).
  --dirty                Copy the current working tree instead of
                         fetching SOURCE_REVISION (passed through to
                         submit).
  --help                 Show this help message and exit.
HELPEOF
}

ACTION=""
PASSTHRU_ARGS=()

if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

for arg in "$@"; do
    case "${arg}" in
        --help|-h)
            show_help
            exit 0
            ;;
        --submit)
            ACTION="submit"
            ;;
        --archive-latest)
            ACTION="archive-latest"
            ;;
        --archive-latest-check)
            ACTION="archive-latest-check"
            ;;
        --archive-latest-purge)
            ACTION="archive-latest-purge"
            ;;
        --check-result)
            ACTION="check-result"
            ;;
        --dirty)
            PASSTHRU_ARGS+=("--dirty")
            ;;
        *)
            echo "Unknown option: ${arg}"
            show_help
            exit 1
            ;;
    esac
done

if [ -z "${ACTION}" ]; then
    echo "No action specified."
    show_help
    exit 1
fi

echo "configuration ================================================ ${SECONDS}"
JOBDATE="$(date '+%Y-%m-%d')"
echo "JOBDATE ${JOBDATE}"

JOBNAME="$(basename -s .sh "$0")"
echo "JOBNAME ${JOBNAME}"

JOBPROJECT="$(basename -s .git "$(git remote get-url origin)")"
echo "JOBPROJECT ${JOBPROJECT}"

################################################################################
# --check-result
################################################################################
if [ "${ACTION}" = "check-result" ]; then
    LATEST_LINK="${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/latest"
    if ! [ -e "${LATEST_LINK}" ]; then
        echo "No latest result found at ${LATEST_LINK}"
        exit 1
    fi
    RESULT_DIR="$(realpath "${LATEST_LINK}")"
    echo "Result directory: ${RESULT_DIR}"
    du -h -a "${RESULT_DIR}" | while IFS=$'\t' read -r size path; do
        echo -e "${size}\t$(realpath "${path}")"
    done
    exit 0
fi

################################################################################
# --archive-latest / --archive-latest-check / --archive-latest-purge
################################################################################
if [[ "${ACTION}" == archive-latest* ]]; then
    LATEST_LINK="${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/latest"
    if ! [ -e "${LATEST_LINK}" ]; then
        echo "No latest result found at ${LATEST_LINK}"
        exit 1
    fi
    RESULT_DIR="$(realpath "${LATEST_LINK}")"
    LATEST_DATE="$(basename "${RESULT_DIR}")"
    ARCHIVE_DIR="${HOME}/archive/${JOBPROJECT}/${LATEST_DATE}/${JOBNAME}"

    if [ "${ACTION}" = "archive-latest-check" ]; then
        if ! [ -e "${ARCHIVE_DIR}" ]; then
            echo "No archive found at ${ARCHIVE_DIR}"
            exit 1
        fi
        du -h -a "${ARCHIVE_DIR}" | while IFS=$'\t' read -r size path; do
            echo -e "${size}\t$(realpath "${path}")"
        done
        exit 0
    fi

    if [ "${ACTION}" = "archive-latest-purge" ]; then
        echo "Purging archive at ${ARCHIVE_DIR} ..."
        singularity exec docker://rclone/rclone:1.73 \
            rclone purge "${ARCHIVE_DIR}"
        echo "Done."
        exit 0
    fi

    # ACTION = archive-latest
    if [ -e "${ARCHIVE_DIR}" ]; then
        echo "ERROR: Archive already exists at ${ARCHIVE_DIR}"
        echo "Run --archive-latest-purge first to remove it."
        exit 1
    fi

    echo "Will archive:"
    echo "  FROM: ${RESULT_DIR}"
    echo "  TO:   ${ARCHIVE_DIR}"
    echo ""
    echo "--- DRY RUN ---"
    mkdir -p "${ARCHIVE_DIR}"
    singularity exec docker://rclone/rclone:1.73 \
        rclone -L --transfers 16 --checkers 16 --progress \
        --multi-thread-streams 4 --dry-run \
        --exclude '.**' \
        copy "${RESULT_DIR}" "${ARCHIVE_DIR}"
    echo "--- END DRY RUN ---"
    echo ""
    read -rp "Proceed with archive? [y/N] " confirm
    if [[ "${confirm}" != [yY] ]]; then
        echo "Aborted."
        exit 0
    fi
    mkdir -p "${ARCHIVE_DIR}"
    singularity exec docker://rclone/rclone:1.73 \
        rclone -L --transfers 16 --checkers 16 --progress \
        --multi-thread-streams 4 \
        --exclude '.**' \
        copy "${RESULT_DIR}" "${ARCHIVE_DIR}"
    echo "Archive complete: ${ARCHIVE_DIR}"
    exit 0
fi

################################################################################
# --submit
################################################################################

SOURCE_REVISION="f9c054a01fd961b22731cde6fb22de84e23871d9"
echo "SOURCE_REVISION ${SOURCE_REVISION}"
SOURCE_REMOTE_URL="$(git config --get remote.origin.url)"
echo "SOURCE_REMOTE_URL ${SOURCE_REMOTE_URL}"

echo "initialization telemetry ==================================== ${SECONDS}"
echo "date $(date)"
echo "hostname $(hostname)"
echo "PWD ${PWD}"
echo "SLURM_JOB_ID ${SLURM_JOB_ID:-nojid}"
echo "SLURM_ARRAY_TASK_ID ${SLURM_ARRAY_TASK_ID:-notid}"
module purge || :
module load python/3.8.6 || :
module load Python/3.8.6-GCCcore-10.2.0 || :
echo "python3.8 $(which python3.8)"
echo "python3.8 --version $(python3.8 --version)"

echo "setup HOME dirs ============================================= ${SECONDS}"
mkdir -p "${HOME}/joblatest"
mkdir -p "${HOME}/joblog"
mkdir -p "${HOME}/jobscript"
if ! [ -e "${HOME}/scratch" ]; then
    if [ -e "/mnt/scratch/${USER}" ]; then
        ln -s "/mnt/scratch/${USER}" "${HOME}/scratch" || :
    else
        mkdir -p "${HOME}/scratch" || :
    fi
fi

echo "verify singularity container ================================ ${SECONDS}"
container="docker://ghcr.io/mmore500/hstrat:v1.21.2"
echo "container ${container}"
echo "Checking container is available and cached..."
singularity exec ${container} \
    python3 -m hstrat --version
echo "Container verified."

echo "setup BATCHDIR =============================================== ${SECONDS}"
BATCHDIR="${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/${JOBDATE}"
if [ -e "${BATCHDIR}" ]; then
    echo "BATCHDIR ${BATCHDIR} exists, clearing it"
fi
rm -rf "${BATCHDIR}"
mkdir -p "${BATCHDIR}"
echo "BATCHDIR ${BATCHDIR}"

ln -sfn "${BATCHDIR}" "${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/latest"

BATCHDIR_JOBLOG="${BATCHDIR}/joblog"
echo "BATCHDIR_JOBLOG ${BATCHDIR_JOBLOG}"
mkdir -p "${BATCHDIR_JOBLOG}"

BATCHDIR_JOBRESULT="${BATCHDIR}/jobresult"
echo "BATCHDIR_JOBRESULT ${BATCHDIR_JOBRESULT}"
mkdir -p "${BATCHDIR_JOBRESULT}"

BATCHDIR_JOBSCRIPT="${BATCHDIR}/jobscript"
echo "BATCHDIR_JOBSCRIPT ${BATCHDIR_JOBSCRIPT}"
mkdir -p "${BATCHDIR_JOBSCRIPT}"

BATCHDIR_JOBSOURCE="${BATCHDIR}/_jobsource"
echo "BATCHDIR_JOBSOURCE ${BATCHDIR_JOBSOURCE}"
if [[ " ${PASSTHRU_ARGS[*]:-} " == *" --dirty "* ]]; then
    cp -r "$(git rev-parse --show-toplevel)" "${BATCHDIR_JOBSOURCE}"
else
    mkdir -p "${BATCHDIR_JOBSOURCE}"
    for attempt in {1..5}; do
        rm -rf "${BATCHDIR_JOBSOURCE}/.git"
        git -C "${BATCHDIR_JOBSOURCE}" init \
        && git -C "${BATCHDIR_JOBSOURCE}" remote add origin "${SOURCE_REMOTE_URL}" \
        && git -C "${BATCHDIR_JOBSOURCE}" fetch origin "${SOURCE_REVISION}" --depth=1 \
        && git -C "${BATCHDIR_JOBSOURCE}" reset --hard FETCH_HEAD \
        && break || echo "failed to clone, retrying..."
        if [ $attempt -eq 5 ]; then
            echo "failed to clone, failing"
            exit 1
        fi
        sleep 5
    done
fi

BATCHDIR_ENV="${BATCHDIR}/.env"
python3.8 -m venv --system-site-packages "${BATCHDIR_ENV}"
source "${BATCHDIR_ENV}/bin/activate"
echo "python3.8 $(which python3.8)"
echo "python3.8 --version $(python3.8 --version)"
for attempt in {1..5}; do
    python3.8 -m pip install --upgrade pip setuptools wheel || :
    python3.8 -m pip install --upgrade \
        "${BATCHDIR_JOBSOURCE}" \
        "joinem==0.9.1" \
        "polars[pyarrow]==1.8.2" \
        "polars-u64-idx==1.8.2" \
    && break || echo "pip install attempt ${attempt} failed"
    if [ ${attempt} -eq 3 ]; then
        echo "pip install failed"
        exit 1
    fi
done

echo "setup dependencies =========================================== ${SECONDS}"
source "${BATCHDIR_ENV}/bin/activate"
python3.8 -m pip freeze

echo "sbatch preamble ============================================== ${SECONDS}"
JOB_PREAMBLE=$(cat << EOF
set -euo pipefail
shopt -s globstar

# adapted from https://unix.stackexchange.com/a/504829
handlefail() {
    echo ">>>error<<<" || :
    awk 'NR>L-4 && NR<L+4 { printf "%-5d%3s%s\n",NR,(NR==L?">>>":""),\$0 }' L=\$1 \$0 || :
    ln -sfn "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.failed" || :
    ln -sfn "\${JOBLOG}" "\${HOME}/joblatest/joblog.failed" || :
    $(which scontrol || which echo) requeuehold "${SLURM_JOBID:-nojid}"
}
trap 'handlefail $LINENO' ERR

echo "initialization telemetry ------------------------------------ \${SECONDS}"
echo "SOURCE_REVISION ${SOURCE_REVISION}"
echo "BATCHDIR ${BATCHDIR}"

echo "cc SLURM script --------------------------------------------- \${SECONDS}"
JOBSCRIPT="\${HOME}/jobscript/\${SLURM_JOB_ID:-nojid}"
echo "JOBSCRIPT \${JOBSCRIPT}"
cp "\${0}" "\${JOBSCRIPT}"
chmod +x "\${JOBSCRIPT}"
cp "\${JOBSCRIPT}" "${BATCHDIR_JOBSCRIPT}/\${SLURM_JOB_ID:-nojid}"
ln -sfn "\${JOBSCRIPT}" "${HOME}/joblatest/jobscript.launched"

echo "cc job log -------------------------------------------------- \${SECONDS}"
JOBLOG="\${HOME}/joblog/\${SLURM_JOB_ID:-nojid}"
echo "JOBLOG \${JOBLOG}"
touch "\${JOBLOG}"
ln -sfn "\${JOBLOG}" "${BATCHDIR_JOBLOG}/\${SLURM_JOB_ID:-nojid}"
ln -sfn "\${JOBLOG}" "\${HOME}/joblatest/joblog.launched"

echo "setup JOBDIR ------------------------------------------------ \${SECONDS}"
JOBDIR="${BATCHDIR}/__\${SLURM_ARRAY_TASK_ID:-\${SLURM_JOB_ID:-\${RANDOM}}}"
echo "JOBDIR \${JOBDIR}"
if [ -e "\${JOBDIR}" ]; then
    echo "JOBDIR \${JOBDIR} exists, clearing it"
fi
rm -rf "\${JOBDIR}"
mkdir -p "\${JOBDIR}"
cd "\${JOBDIR}"
echo "PWD \${PWD}"

echo "job telemetry ----------------------------------------------- \${SECONDS}"
echo "source SLURM_JOB_ID ${SLURM_JOB_ID:-nojid}"
echo "current SLURM_JOB_ID \${SLURM_JOB_ID:-nojid}"
echo "SLURM_ARRAY_TASK_ID \${SLURM_ARRAY_TASK_ID:-notid}"
echo "hostname \$(hostname)"
echo "date \$(date)"

echo "module setup ------------------------------------------------ \${SECONDS}"
module purge || :
module load python/3.8.6 || :
module load Python/3.8.6-GCCcore-10.2.0 || :
echo "python3.8 \$(which python3.8)"
echo "python3.8 --version \$(python3.8 --version)"

echo "setup dependencies- ----------------------------------------- \${SECONDS}"
source "${BATCHDIR_ENV}/bin/activate"
python3.8 -m pip freeze

EOF
)

echo "create sbatch file: work ==================================== ${SECONDS}"

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# WORK ---------------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash -login
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=750G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%A_%a"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --array=0-1
#SBATCH --account=ecode

${JOB_PREAMBLE}

echo "lscpu ------------------------------------------------------- \${SECONDS}"
lscpu || :

echo "lshw -------------------------------------------------------- \${SECONDS}"
lshw || :

echo "cpuinfo ----------------------------------------------------- \${SECONDS}"
cat /proc/cpuinfo || :

echo "configure replicate ----------------------------------------- \${SECONDS}"
config=\$(python3.8 << EOF_
import itertools as it
import os

dummy_sources = [
    "https://github.com/mmore500/hstrat-reconstruction-algo/raw/refs/heads/main/assets/2024-12-25/a=genomes+flavor=genome_purifyingonly+seed=1+ncycle=5000000+ext=.pqt",
    "https://github.com/mmore500/hstrat-reconstruction-algo/raw/refs/heads/main/assets/2024-12-25/a=genomes+flavor=genome_purifyingplus+seed=1+ncycle=5000000+ext=.pqt",
]

real_sources = [
    "${HOME}/2026-02-20/a=fossils+flavor=purifying+ext=.pqt",
    "${HOME}/2026-02-20/a=fossils+flavor=sweeping+ext=.pqt",
]

replicates = it.product(
    real_sources if "SLURM_ARRAY_TASK_ID" in os.environ else dummy_sources,
    [
        1_000_000_000,
    ] if "SLURM_ARRAY_TASK_ID" in os.environ else [1_000],
)

source, num_tips = next(it.islice(replicates, \${SLURM_ARRAY_TASK_ID:-0}, None))
print(f"phylo_source_path='{source}' num_tips='{num_tips}'")
EOF_
)

echo "config \${config}"
eval "\${config}"

echo "phylo_source_path \${phylo_source_path}"
echo "num_tips \${num_tips}"

echo "realpath phylo_source_path \$(realpath \${phylo_source_path})"

echo "configure --------------------------------------------------- \${SECONDS}"
echo "LOCAL \${LOCAL:-}"
echo "TMPDIR \${TMPDIR:-}"
MYLOCAL="\${LOCAL:-\${TMPDIR:-.}}/\$(uuidgen)"
echo "MYLOCAL \${MYLOCAL}"
mkdir -p "\${MYLOCAL}"

export APPTAINER_BINDPATH="\$(realpath \${MYLOCAL}):/local:rw"
export SINGULARITY_BINDPATH="\$(realpath \${MYLOCAL}):/local:rw"

genomes_inpath="\${MYLOCAL}/genomes.pqt"
phylo_outpath="\${MYLOCAL}/phylo.pqt"
echo "genomes_inpath \${genomes_inpath}"
echo "phylo_outpath \${phylo_outpath}"

if [ -f "\${phylo_source_path}" ]; then
    echo "phylo_source_path exists, copying into place"
    phylo_source_path="\$(realpath \${phylo_source_path})"
    echo "phylo_source_path \${phylo_source_path}"
    ls -l "\${phylo_source_path}"
    cp "\${phylo_source_path}" "\${genomes_inpath}"
    ls -l "\${genomes_inpath}"
else
    echo "phylo_source_path does not exist, downloading"
    wget -O "\${genomes_inpath}" "\${phylo_source_path}"
    ls -l "\${genomes_inpath}"
fi

du -h "\${genomes_inpath}"

echo "extracting \${genomes_inpath} tail \${num_tips} rows..."
ls -1 "\${genomes_inpath}" | \
    singularity run docker://ghcr.io/mmore500/joinem:v0.11.1 \
    "\${MYLOCAL}/tailgenomes.pqt" \
    --tail "\${num_tips}"
echo "... done!"

mv "\${MYLOCAL}/tailgenomes.pqt" "\${genomes_inpath}"

echo "downsampling genomes to 50_000_000 fossils ------------------- \${SECONDS}"
ls -1 "\${genomes_inpath}" | \
    singularity run docker://ghcr.io/mmore500/joinem:v0.11.1 \
    "\${MYLOCAL}/sampledgenomes.pqt" \
    --sample 50_000_000 \
    --seed 1
echo "... done!"

mv "\${MYLOCAL}/sampledgenomes.pqt" "\${genomes_inpath}"
du -h "\${genomes_inpath}"

container="docker://ghcr.io/mmore500/hstrat:v1.21.2"
echo "container \${container}"

export PYTHONUNBUFFERED=1
export SINGULARITYENV_PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=30
export NUMBA_NUM_THREADS=30
export TQDM_MININTERVAL=5

echo "test container ---------------------------------------------- \${SECONDS}"
singularity exec \${container} \
    python3 -O -m hstrat.dataframe.surface_unpack_reconstruct --help

echo "do work ----------------------------------------------------- \${SECONDS}"
echo "warmup jit cache"
warmup_outpath="/tmp/\$(uuidgen).pqt"
echo "/local/\$(basename "\${genomes_inpath}")" \
    | singularity exec \${container} \
        python3 -O -m hstrat.dataframe.surface_unpack_reconstruct \
        "\${warmup_outpath}" \
        --tail 100

echo "do reconstruction and postprocessing"
stdbuf -e0 -i0 -o0 echo "/local/\$(basename "\${genomes_inpath}")" \
    | stdbuf -o0 singularity exec \${container} \
        python3 -O -m hstrat.dataframe.surface_unpack_reconstruct \
        "/local/\$(basename "\${phylo_outpath}")" \
        --no-drop-dstream-metadata \
        --collapse-unif-freq 7 \
        --exploded-slice-size 5_000_000 \
        --check-trie-invariant-freq=1 \
        --collapse-unif-freq=1 --check-trie-invariant-after-collapse-unif \
        --shrink-dtypes --eager-write \
        --write-kwarg 'compression="lz4"' \
        2>&1 \
    | stdbuf -e0 -i0 -o0 tr '\r' '\n' \
    | stdbuf -e0 -i0 -o0 python3.8 -m pylib.script.tee_eval_context_durations \
        -o "\${JOBDIR}/a=result+ext=.csv" \
        --with-column "pl.lit('\${phylo_source_path}').alias('phylo_source_path')" \
        --with-column "pl.lit('${SOURCE_REVISION}').alias('revision')" \
        --with-column 'pl.lit(64).alias("dstream_S")' \
        --with-column 'pl.lit(1).alias("dstream_value_bitwidth")' \
        --with-column "pl.lit(\${num_tips}).alias('num_tips')"

ls "\${MYLOCAL}"

echo "cleanup"
du -h "\${phylo_outpath}"
cp "\${phylo_outpath}" "\${JOBDIR}/a=phylo+ext=.pqt"

echo "finalization telemetry -------------------------------------- \${SECONDS}"
ls -l \${JOBDIR}
du -h \${JOBDIR}
ln -sfn "\${JOBSCRIPT}" "${HOME}/joblatest/jobscript.finished"
ln -sfn "\${JOBLOG}" "${HOME}/joblatest/joblog.finished"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'

EOF
###############################################################################
# --------------------------------------------------------------------------- #
###############################################################################


echo "submit sbatch file ========================================== ${SECONDS}"
$(which sbatch && echo --job-name="${JOBNAME}" || which bash) "${SBATCH_FILE}"

echo "create sbatch file: collate ================================= ${SECONDS}"

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# COLLATE ------------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash -login
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=750G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --account=ecode
#SBATCH --requeue

${JOB_PREAMBLE}

echo "BATCHDIR ${BATCHDIR}"
ls -l "${BATCHDIR}"

container="docker://ghcr.io/mmore500/hstrat:v1.21.3"
echo "container \${container}"

export PYTHONUNBUFFERED=1
export SINGULARITYENV_PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=30
export NUMBA_NUM_THREADS=30
export TQDM_MININTERVAL=5

echo "finalize ---------------------------------------------------- \${SECONDS}"
echo "   - join result"
ls -1 "${BATCHDIR}"/__*/**/a=result+* \
    | tee /dev/stderr \
    | python3.8 -m joinem --progress \
        "${BATCHDIR_JOBRESULT}/a=result+date=${JOBDATE}+job=${JOBNAME}+ext=.csv"
ls -l "${BATCHDIR_JOBRESULT}"
du -h "${BATCHDIR_JOBRESULT}"
head "${BATCHDIR_JOBRESULT}/a=result+date=${JOBDATE}+job=${JOBNAME}+ext=.csv"

echo "   - SKIP archive job dir"

echo "   - archive joblog"
pushd "${BATCHDIR}"
    tar czf \
    "${BATCHDIR_JOBRESULT}/a=joblog+date=${JOBDATE}+job=${JOBNAME}+ext=.tar.gz" \
    -h "$(basename "${BATCHDIR_JOBLOG}")"
popd

echo "   - archive jobscript"
pushd "${BATCHDIR}"
    tar czfv \
    "$(basename "${BATCHDIR_JOBRESULT}")/a=jobscript+date=${JOBDATE}+job=${JOBNAME}+ext=.tar.gz" \
    -h "$(basename ${BATCHDIR_JOBSCRIPT})"
popd

ls -l "${BATCHDIR}"

echo "downsample and convert -------------------------------------- \${SECONDS}"
for phylo_path in "${BATCHDIR}"/__*/**/a=phylo+ext=.pqt; do
    echo "============================================================"
    echo "processing \${phylo_path}"
    echo "============================================================"

    phylo_dir="\$(dirname "\${phylo_path}")"
    echo "phylo_dir \${phylo_dir}"

    echo "copying \${phylo_path} to /tmp"
    cp "\${phylo_path}" "/tmp/\${SLURM_JOB_ID:-nojid}_source.pqt"
    source_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_source.pqt"

    ############################################################################
    # 1) downsample tips: 50k random
    ############################################################################
    echo "--- downsample tips 50k ----------------------------------- \${SECONDS}"
    tmp_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_dsamp.pqt"
    echo "\${source_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_downsample_tips_polars \
            "\${tmp_pqt}" \
            -n 50000 \
            --seed 1 --eager-write
    echo "\${tmp_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_collapse_unifurcations_polars \
            "\${tmp_pqt}" \
            --eager-write
    echo "\${tmp_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_assign_contiguous_ids_polars \
            "\${tmp_pqt}" \
            --eager-write
    mv "\${tmp_pqt}" "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.pqt"
    singularity exec \${container} \
        python3 -m hstrat._auxiliary_lib._alifestd_as_newick_polars \
        -i "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.pqt" \
        -o "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.nwk"
    ls -l "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.pqt"
    ls -l "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.nwk"
    du -h "\${phylo_dir}/a=phylo+dsamp=tips50k+ext=.pqt"

    ############################################################################
    # 2) downsample tips canopy: criterion "layer", no -n
    ############################################################################
    echo "--- downsample tips canopy -------------------------------- \${SECONDS}"
    tmp_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_canopy.pqt"
    echo "\${source_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_downsample_tips_canopy_polars \
            "\${tmp_pqt}" \
            --criterion "layer" \
            --seed 1 --eager-write
    echo "\${tmp_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_collapse_unifurcations_polars \
            "\${tmp_pqt}" \
            --eager-write
    echo "\${tmp_pqt}" \
        | singularity run \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_assign_contiguous_ids_polars \
            "\${tmp_pqt}" \
            --eager-write
    mv "\${tmp_pqt}" "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.pqt"
    singularity exec \${container} \
        python3 -m hstrat._auxiliary_lib._alifestd_as_newick_polars \
        -i "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.pqt" \
        -o "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.nwk"
    ls -l "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.pqt"
    ls -l "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.nwk"
    du -h "\${phylo_dir}/a=phylo+dsamp=canopy+criterion=layer+ext=.pqt"

    ############################################################################
    # 3) downsample tips lineage polars: 10k tips, seeds 1-5
    ############################################################################
    for seed in 1 2 3 4 5; do
        echo "--- downsample tips lineage seed=\${seed} ----------------- \${SECONDS}"
        tmp_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_lineage_s\${seed}.pqt"
        echo "\${source_pqt}" \
            | singularity run \${container} \
                python3 -m hstrat._auxiliary_lib._alifestd_downsample_tips_lineage_polars \
                "\${tmp_pqt}" \
                -n 10000 \
                --criterion-delta "dstream_rank" \
                --criterion-target "layer" \
                --seed "\${seed}" --eager-write
        echo "\${tmp_pqt}" \
            | singularity run \${container} \
                python3 -m hstrat._auxiliary_lib._alifestd_collapse_unifurcations_polars \
                "\${tmp_pqt}" \
                --eager-write
        echo "\${tmp_pqt}" \
            | singularity run \${container} \
                python3 -m hstrat._auxiliary_lib._alifestd_assign_contiguous_ids_polars \
                "\${tmp_pqt}" \
                --eager-write
        mv "\${tmp_pqt}" "\${phylo_dir}/a=phylo+dsamp=lineage10k+cdelta=dstream_rank+ctarget=layer+seed=\${seed}+ext=.pqt"
        singularity exec \${container} \
            python3 -m hstrat._auxiliary_lib._alifestd_as_newick_polars \
            -i "\${phylo_dir}/a=phylo+dsamp=lineage10k+cdelta=dstream_rank+ctarget=layer+seed=\${seed}+ext=.pqt" \
            -o "\${phylo_dir}/a=phylo+dsamp=lineage10k+cdelta=dstream_rank+ctarget=layer+seed=\${seed}+ext=.nwk"
        ls -l "\${phylo_dir}/a=phylo+dsamp=lineage10k+cdelta=dstream_rank+ctarget=layer+seed=\${seed}+ext=.pqt"
        du -h "\${phylo_dir}/a=phylo+dsamp=lineage10k+cdelta=dstream_rank+ctarget=layer+seed=\${seed}+ext=.pqt"
    done

    ############################################################################
    # 4) downsample tips lineage stratified polars:
    #    seeds 1-5, n_tips_per_stratum 1 and 4
    ############################################################################
    for seed in 1 2 3 4 5; do
        for ntps in 1 4; do
            echo "--- downsample tips lineage stratified seed=\${seed} ntps=\${ntps} --- \${SECONDS}"
            tmp_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_linstrat_s\${seed}_ntps\${ntps}.pqt"
            echo "\${source_pqt}" \
                | singularity run \${container} \
                    python3 -m hstrat._auxiliary_lib._alifestd_downsample_tips_lineage_stratified_polars \
                    "\${tmp_pqt}" \
                    --criterion-delta "dstream_rank" \
                    --criterion-target "layer" \
                    --criterion-stratify "layer" \
                    --n-tips-per-stratum "\${ntps}" \
                    --seed "\${seed}" --eager-write
            echo "\${tmp_pqt}" \
                | singularity run \${container} \
                    python3 -m hstrat._auxiliary_lib._alifestd_collapse_unifurcations_polars \
                    "\${tmp_pqt}" \
                    --eager-write
            echo "\${tmp_pqt}" \
                | singularity run \${container} \
                    python3 -m hstrat._auxiliary_lib._alifestd_assign_contiguous_ids_polars \
                    "\${tmp_pqt}" \
                    --eager-write
            mv "\${tmp_pqt}" "\${phylo_dir}/a=phylo+dsamp=lineage-stratified+cdelta=dstream_rank+ctarget=layer+cstratify=layer+ntps=\${ntps}+seed=\${seed}+ext=.pqt"
            singularity exec \${container} \
                python3 -m hstrat._auxiliary_lib._alifestd_as_newick_polars \
                -i "\${phylo_dir}/a=phylo+dsamp=lineage-stratified+cdelta=dstream_rank+ctarget=layer+cstratify=layer+ntps=\${ntps}+seed=\${seed}+ext=.pqt" \
                -o "\${phylo_dir}/a=phylo+dsamp=lineage-stratified+cdelta=dstream_rank+ctarget=layer+cstratify=layer+ntps=\${ntps}+seed=\${seed}+ext=.nwk"
            ls -l "\${phylo_dir}/a=phylo+dsamp=lineage-stratified+cdelta=dstream_rank+ctarget=layer+cstratify=layer+ntps=\${ntps}+seed=\${seed}+ext=.pqt"
            du -h "\${phylo_dir}/a=phylo+dsamp=lineage-stratified+cdelta=dstream_rank+ctarget=layer+cstratify=layer+ntps=\${ntps}+seed=\${seed}+ext=.pqt"
        done
    done

    echo "cleaning up source copy"
    rm -f "\${source_pqt}"
done

echo "validate trie ----------------------------------------------- \${SECONDS}"
for phylo_path in "${BATCHDIR}"/__*/**/a=phylo+ext=.pqt; do
    echo "validating \${phylo_path}"
    timeout 1800 singularity exec \${container} \
        python3 -m hstrat.dataframe.surface_validate_trie \
        "\${phylo_path}" \
        --max-num-checks 1000 \
        --seed 1 \
    && echo "validation passed for \${phylo_path}" \
    || {
        rc=\$?
        if [ \${rc} -eq 124 ]; then
            echo "validation timed out after 30 minutes (considered success)"
        else
            echo "validation FAILED for \${phylo_path} (exit code \${rc})"
            exit 1
        fi
    }
done

echo "cleanup ----------------------------------------------------- \${SECONDS}"
echo "skipping cleanup"
cd
ls -l "${BATCHDIR}"

echo "finalization telemetry -------------------------------------- \${SECONDS}"
ln -sfn "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.completed"
ln -sfn "\${JOBLOG}" "\${HOME}/joblatest/joblog.completed"
ln -sfn "${BATCHDIR_JOBRESULT}" "\${HOME}/joblatest/jobresult.completed"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'

EOF
###############################################################################
# --------------------------------------------------------------------------- #
###############################################################################

echo "submit sbatch file ========================================== ${SECONDS}"
$(which sbatch && echo --job-name="${JOBNAME}" --dependency=singleton || which bash) "${SBATCH_FILE}"

echo "finalization telemetry ====================================== ${SECONDS}"
echo "BATCHDIR ${BATCHDIR}"
echo "SECONDS ${SECONDS}"
echo '>>>complete<<<'

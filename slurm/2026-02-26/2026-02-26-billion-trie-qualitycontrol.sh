#!/bin/bash -login

set -euo pipefail

cd "$(dirname "$0")"

################################################################################
# Container configuration
################################################################################
HSTRAT_CONTAINER="docker://ghcr.io/mmore500/hstrat:v1.21.5"

################################################################################
# CLI flag handling
################################################################################
show_help() {
    cat << 'HELPEOF'
Usage: 2026-02-26-billion-trie-qualitycontrol.sh [OPTIONS]

Workflow to build a billion-tip trie with quality control checks.

Options:
  --submit               Submit the full SLURM workflow (reconstruct +
                         downsample + cleanup).
  --submit-downsample    Submit only the downsampling and cleanup jobs
                         (reuses existing reconstruction results).
  --submit-cleanup       Submit only the cleanup/collate job (reuses
                         existing reconstruction and downsampled results).
  --submit-validation    Submit only the validation jobs (reuses
                         existing reconstruction results).
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
        --submit-downsample)
            ACTION="submit-downsample"
            ;;
        --submit-cleanup)
            ACTION="submit-cleanup"
            ;;
        --submit-validation)
            ACTION="submit-validation"
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
# --submit / --submit-downsample / --submit-cleanup
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
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
echo "Checking container is available and cached..."
singularity exec "${HSTRAT_CONTAINER}" \
    python3 -m hstrat --version
echo "Container verified."

echo "verify rclone container ===================================== ${SECONDS}"
echo "Checking rclone container is available and cached..."
singularity exec docker://rclone/rclone:1.73 \
    rclone --version
echo "rclone container verified."

echo "setup BATCHDIR =============================================== ${SECONDS}"
if [ "${ACTION}" = "submit" ]; then
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
else
    # --submit-downsample or --submit-cleanup: reuse existing BATCHDIR
    LATEST_LINK="${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/latest"
    if ! [ -e "${LATEST_LINK}" ]; then
        echo "No latest BATCHDIR found at ${LATEST_LINK}"
        echo "Run --submit first to create the reconstruction batch."
        exit 1
    fi
    BATCHDIR="$(realpath "${LATEST_LINK}")"
    echo "BATCHDIR ${BATCHDIR} (reusing existing)"

    BATCHDIR_JOBLOG="${BATCHDIR}/joblog"
    BATCHDIR_JOBRESULT="${BATCHDIR}/jobresult"
    BATCHDIR_JOBSCRIPT="${BATCHDIR}/jobscript"
    BATCHDIR_ENV="${BATCHDIR}/.env"

    echo "setup dependencies =========================================== ${SECONDS}"
    source "${BATCHDIR_ENV}/bin/activate"
    python3.8 -m pip freeze
fi

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
    \$(which scontrol || which echo) requeuehold "\${SLURM_JOB_ID:-nojid}"
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
if [ -n "\${JOBSUBDIR:-}" ]; then
    JOBDIR="${BATCHDIR}/\${JOBSUBDIR}/\${SLURM_ARRAY_TASK_ID:-\${SLURM_JOB_ID:-\${RANDOM}}}"
else
    JOBDIR="${BATCHDIR}/.\${SLURM_ARRAY_TASK_ID:-\${SLURM_JOB_ID:-\${RANDOM}}}"
fi
echo "JOBDIR \${JOBDIR}"
if [ "\${CLEAR_JOBDIR:-1}" = "1" ]; then
    if [ -e "\${JOBDIR}" ]; then
        echo "JOBDIR \${JOBDIR} exists, clearing it"
    fi
    rm -rf "\${JOBDIR}"
fi
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
#SBATCH --cpus-per-task=64
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

export PYTHONUNBUFFERED=1
export SINGULARITYENV_PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=30
export NUMBA_NUM_THREADS=30
export TQDM_MININTERVAL=5

echo "test container ---------------------------------------------- \${SECONDS}"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
singularity exec ${HSTRAT_CONTAINER} \
    python3 -O -m hstrat.dataframe.surface_unpack_reconstruct --help

echo "do work ----------------------------------------------------- \${SECONDS}"
echo "warmup jit cache"
warmup_outpath="/tmp/\$(uuidgen).pqt"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
echo "/local/\$(basename "\${genomes_inpath}")" \
    | singularity exec ${HSTRAT_CONTAINER} \
        python3 -O -m hstrat.dataframe.surface_unpack_reconstruct \
        "\${warmup_outpath}" \
        --tail 100

echo "do reconstruction"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
stdbuf -e0 -i0 -o0 echo "/local/\$(basename "\${genomes_inpath}")" \
    | stdbuf -o0 singularity exec ${HSTRAT_CONTAINER} \
        python3 -O -m hstrat.dataframe.surface_unpack_reconstruct \
        "/local/\$(basename "\${phylo_outpath}")" \
        --no-drop-dstream-metadata \
        --collapse-unif-freq 7 \
        --exploded-slice-size 50_000_000 \
        --check-trie-invariant-freq 7 \
        --check-trie-invariant-after-collapse-unif \
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


echo "submit work job ============================================= ${SECONDS}"
WORK_JOBID=""
if [ "${ACTION}" = "submit" ]; then
    if command -v sbatch &>/dev/null; then
        WORK_JOBID=$(sbatch --parsable --job-name="${JOBNAME}-work" "${SBATCH_FILE}")
        echo "Submitted WORK job: ${WORK_JOBID}"
    else
        bash "${SBATCH_FILE}"
    fi
fi

echo "create sbatch file: validation ============================== ${SECONDS}"

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# VALIDATION ---------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash -login
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=250G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%A_%a"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=FAIL
#SBATCH --array=0-9
#SBATCH --account=ecode

export CLEAR_JOBDIR=0
export JOBSUBDIR=".validation"

${JOB_PREAMBLE}

SEED=\${SLURM_ARRAY_TASK_ID:-0}
echo "SEED \${SEED}"
VALIDATION_LOG="\${JOBDIR}/validation-seed-\$(printf '%02d' \${SEED}).log"
validation_failed=0

echo "validate trie ----------------------------------------------- \${SECONDS}"
phylo_idx=0
for phylo_path in "${BATCHDIR}"/.[0-9]*/**/a=phylo+ext=.pqt; do
    echo "rclone \${phylo_path} to /tmp"
    tmp_phylo="/tmp/\${SLURM_JOB_ID:-nojid}_validate_\${phylo_idx}.pqt"
    phylo_idx=\$((phylo_idx + 1))
    singularity exec docker://rclone/rclone:1.73 \
        rclone copyto "\${phylo_path}" "\${tmp_phylo}"
    ls -l "\${tmp_phylo}"
    du -h "\${tmp_phylo}"

    echo "validating \${phylo_path} (via \${tmp_phylo}) with seed \${SEED}"
    echo "=== validating \${phylo_path} with seed \${SEED} ===" >> "\${VALIDATION_LOG}"
    echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
    rc=0
    timeout 600 singularity exec ${HSTRAT_CONTAINER} \
        python3 -m hstrat.dataframe.surface_validate_trie \
        "\${tmp_phylo}" \
        --max-num-checks 1000 \
        --seed "\${SEED}" \
        >> "\${VALIDATION_LOG}" 2>&1 \
    || rc=\$?

    if [ \${rc} -eq 0 ]; then
        echo "validation passed for \${phylo_path}"
    elif [ \${rc} -eq 124 ] || [ \${rc} -eq 137 ]; then
        echo "validation timed out (exit code \${rc}, considered success)"
    else
        echo "validation FAILED for \${phylo_path} (exit code \${rc})"
        validation_failed=1
    fi

    echo "cleanup tmp_phylo \${tmp_phylo}"
    rm -f "\${tmp_phylo}"
done

if [ \${validation_failed} -ne 0 ]; then
    cp "\${VALIDATION_LOG}" "${BATCHDIR_JOBRESULT}/failed-validation-seed-\$(printf '%02d' \${SEED}).log"
    echo "Validation failed, log copied to result output"
    exit 1
fi

echo "finalization telemetry -------------------------------------- \${SECONDS}"
ls -l \${JOBDIR}
du -h \${JOBDIR}
ln -sfn "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.finished"
ln -sfn "\${JOBLOG}" "\${HOME}/joblatest/joblog.finished"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'

EOF
###############################################################################
# --------------------------------------------------------------------------- #
###############################################################################

echo "submit validation job ======================================== ${SECONDS}"
VALIDATION_JOBID=""
if [ "${ACTION}" = "submit" ] || [ "${ACTION}" = "submit-validation" ]; then
    if command -v sbatch &>/dev/null; then
        DEP_ON_WORK_VALIDATE=""
        if [ -n "${WORK_JOBID}" ]; then
            DEP_ON_WORK_VALIDATE="--dependency=afterok:${WORK_JOBID}"
        fi
        VALIDATION_JOBID=$(sbatch --parsable --job-name="${JOBNAME}-validate" ${DEP_ON_WORK_VALIDATE} "${SBATCH_FILE}")
        echo "Submitted VALIDATION job: ${VALIDATION_JOBID}"
    else
        bash "${SBATCH_FILE}"
    fi
fi

echo "downsample sbatch template ================================== ${SECONDS}"
# Template for downsampling jobs; placeholders:
#   __DSAMP_LABEL__  - human-readable label for this task
#   __DSAMP_MODULE__ - hstrat module suffix (e.g. _alifestd_downsample_tips_polars)
#   __DSAMP_ARGS__   - extra CLI arguments for the downsample command
#   __DSAMP_OUTNAME__ - output filename stem (without .pqt/.nwk extension)
DSAMP_TEMPLATE=$(cat << DSAMP_TMPLEOF
#!/bin/bash -login
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=250G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%A_%a"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=FAIL
#SBATCH --array=0-1
#SBATCH --account=ecode

export CLEAR_JOBDIR=0
export JOBSUBDIR="dsamp-__DSAMP_LABEL__"

${JOB_PREAMBLE}

echo "downsample: __DSAMP_LABEL__ -------------------------------- \${SECONDS}"

phylo_path="${BATCHDIR}/.\${SLURM_ARRAY_TASK_ID:-0}/a=phylo+ext=.pqt"
echo "phylo_path \${phylo_path}"

echo "rclone \${phylo_path} to /tmp"
singularity exec docker://rclone/rclone:1.73 \
    rclone copyto "\${phylo_path}" "/tmp/\${SLURM_JOB_ID:-nojid}_source.pqt"
source_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_source.pqt"
tmp_pqt="/tmp/\${SLURM_JOB_ID:-nojid}_dsamp.pqt"

echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
echo "\${source_pqt}" \
    | singularity exec ${HSTRAT_CONTAINER} \
        python3 -m hstrat._auxiliary_lib.__DSAMP_MODULE__ \
        "\${tmp_pqt}" \
        __DSAMP_ARGS__ --eager-write

echo "collapse unifurcations -------------------------------------- \${SECONDS}"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
echo "\${tmp_pqt}" \
    | singularity exec ${HSTRAT_CONTAINER} \
        python3 -m hstrat._auxiliary_lib._alifestd_collapse_unifurcations_polars \
        "\${tmp_pqt}" \
        --eager-write

echo "assign contiguous IDs --------------------------------------- \${SECONDS}"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
echo "\${tmp_pqt}" \
    | singularity exec ${HSTRAT_CONTAINER} \
        python3 -m hstrat._auxiliary_lib._alifestd_assign_contiguous_ids_polars \
        "\${tmp_pqt}" \
        --eager-write

echo "move and convert to newick ---------------------------------- \${SECONDS}"
mv "\${tmp_pqt}" "\${JOBDIR}/__DSAMP_OUTNAME__.pqt"
echo "HSTRAT_CONTAINER ${HSTRAT_CONTAINER}"
singularity exec ${HSTRAT_CONTAINER} \
    python3 -m hstrat._auxiliary_lib._alifestd_as_newick_polars \
    -i "\${JOBDIR}/__DSAMP_OUTNAME__.pqt" \
    -o "\${JOBDIR}/__DSAMP_OUTNAME__.nwk"
ls -l "\${JOBDIR}/__DSAMP_OUTNAME__.pqt"
ls -l "\${JOBDIR}/__DSAMP_OUTNAME__.nwk"
du -h "\${JOBDIR}/__DSAMP_OUTNAME__.pqt"

echo "cleanup"
rm -f "\${source_pqt}"

echo "finalization telemetry -------------------------------------- \${SECONDS}"
ls -l \${JOBDIR}
du -h \${JOBDIR}
ln -sfn "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.finished"
ln -sfn "\${JOBLOG}" "\${HOME}/joblatest/joblog.finished"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'
DSAMP_TMPLEOF
)

echo "create and submit downsample jobs ============================ ${SECONDS}"
DSAMP_JOBIDS=()
if [ "${ACTION}" = "submit" ] || [ "${ACTION}" = "submit-downsample" ]; then
    # Build per-task arrays: label, output name, module, extra args
    dsamp_labels=()
    dsamp_outnames=()
    dsamp_modules=()
    dsamp_args=()

    # 1) tips 50k random
    dsamp_labels+=("tips50k")
    dsamp_outnames+=("a=phylo+dsamp=tips50k+ext=")
    dsamp_modules+=("_alifestd_downsample_tips_polars")
    dsamp_args+=("-n 50000 --seed 1")

    # 2) canopy criterion=layer
    dsamp_labels+=("canopy-layer")
    dsamp_outnames+=("a=phylo+criterion=layer+dsamp=canopy+ext=")
    dsamp_modules+=("_alifestd_downsample_tips_canopy_polars")
    dsamp_args+=("--criterion \"layer\" --seed 1")

    # 3) lineage 10k tips, seeds 1-5
    for seed in 1 2 3 4 5; do
        dsamp_labels+=("lineage10k-s${seed}")
        dsamp_outnames+=("a=phylo+cdelta=dstream_rank+ctarget=layer+dsamp=lineage10k+seed=${seed}+ext=")
        dsamp_modules+=("_alifestd_downsample_tips_lineage_polars")
        dsamp_args+=("-n 10000 --criterion-delta \"dstream_rank\" --criterion-target \"layer\" --seed ${seed}")
    done

    # 4) lineage stratified, seeds 1-5, n_tips_per_stratum 1 and 4
    for seed in 1 2 3 4 5; do
        for ntps in 1 4; do
            dsamp_labels+=("lineage-stratified-s${seed}-ntps${ntps}")
            dsamp_outnames+=("a=phylo+cdelta=dstream_rank+cstratify=layer+ctarget=layer+dsamp=lineage-stratified+ntps=${ntps}+seed=${seed}+ext=")
            dsamp_modules+=("_alifestd_downsample_tips_lineage_stratified_polars")
            dsamp_args+=("--criterion-delta \"dstream_rank\" --criterion-target \"layer\" --criterion-stratify \"layer\" --n-tips-per-stratum ${ntps} --seed ${seed}")
        done
    done

    # Compute dependency argument for downsampling jobs
    DEP_ON_WORK=""
    if [ -n "${WORK_JOBID}" ]; then
        DEP_ON_WORK="--dependency=afterok:${WORK_JOBID}"
    fi

    # Generate and submit each downsampling job from the template
    for i in "${!dsamp_labels[@]}"; do
        label="${dsamp_labels[$i]}"
        outname="${dsamp_outnames[$i]}"
        module="${dsamp_modules[$i]}"
        args="${dsamp_args[$i]}"

        echo "--- create dsamp job: ${label} --- ${SECONDS}"
        content="${DSAMP_TEMPLATE}"
        content="${content//__DSAMP_LABEL__/${label}}"
        content="${content//__DSAMP_OUTNAME__/${outname}}"
        content="${content//__DSAMP_MODULE__/${module}}"
        content="${content//__DSAMP_ARGS__/${args}}"

        sbatch_file="$(mktemp)"
        printf '%s\n' "${content}" > "${sbatch_file}"
        echo "SBATCH_FILE (${label}) ${sbatch_file}"

        if command -v sbatch &>/dev/null; then
            JOBID=$(sbatch --parsable --job-name="${JOBNAME}-dsamp-${label}" ${DEP_ON_WORK} "${sbatch_file}")
            echo "Submitted dsamp-${label} -> ${JOBID}"
            DSAMP_JOBIDS+=("${JOBID}")
        else
            bash "${sbatch_file}"
        fi
    done
fi

echo "create sbatch file: cleanup ================================= ${SECONDS}"

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# CLEANUP ------------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash -login
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=250G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --account=ecode
#SBATCH --requeue

${JOB_PREAMBLE}

echo "BATCHDIR ${BATCHDIR}"
ls -l "${BATCHDIR}"

export PYTHONUNBUFFERED=1
export SINGULARITYENV_PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=2
export NUMBA_NUM_THREADS=2
export TQDM_MININTERVAL=5

echo "finalize ---------------------------------------------------- \${SECONDS}"
echo "   - join result"
ls -1 "${BATCHDIR}"/.[0-9]*/**/a=result+* \
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

echo "submit cleanup job =========================================== ${SECONDS}"
if [ "${ACTION}" = "submit" ] || [ "${ACTION}" = "submit-downsample" ]; then
    if command -v sbatch &>/dev/null; then
        if [ ${#DSAMP_JOBIDS[@]} -gt 0 ]; then
            DEP_ON_DSAMP="--dependency=afterok:$(IFS=:; echo "${DSAMP_JOBIDS[*]}")"
        else
            DEP_ON_DSAMP=""
        fi
        sbatch --job-name="${JOBNAME}-cleanup" ${DEP_ON_DSAMP} "${SBATCH_FILE}"
    else
        bash "${SBATCH_FILE}"
    fi
elif [ "${ACTION}" = "submit-cleanup" ]; then
    if command -v sbatch &>/dev/null; then
        sbatch --job-name="${JOBNAME}-cleanup" "${SBATCH_FILE}"
    else
        bash "${SBATCH_FILE}"
    fi
fi

echo "finalization telemetry ====================================== ${SECONDS}"
echo "BATCHDIR ${BATCHDIR}"
echo "SECONDS ${SECONDS}"
echo '>>>complete<<<'

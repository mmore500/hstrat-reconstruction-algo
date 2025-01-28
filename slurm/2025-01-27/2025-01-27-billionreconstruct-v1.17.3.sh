#!/bin/bash -login

set -euo pipefail

cd "$(dirname "$0")"

echo "configuration ================================================ ${SECONDS}"
JOBDATE="$(date '+%Y-%m-%d')"
echo "JOBDATE ${JOBDATE}"

JOBNAME="$(basename -s .sh "$0")"
echo "JOBNAME ${JOBNAME}"

JOBPROJECT="$(basename -s .git "$(git remote get-url origin)")"
echo "JOBPROJECT ${JOBPROJECT}"

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
if [[ $* == *--dirty* ]]; then
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

BATCHDIR_ENV="${BATCHDIR}/_jobenv"
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
#SBATCH --cpus-per-task=64
#SBATCH --mem=996G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%A_%a"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --array=0
#SBATCH --account=beacon

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
    "${HOME}/scratch/2024-12-25/lex12+async-ga/wse-sketches+genome-flavor=genome_purifyingonly+seed=1/kernel-async-ga/a=fossils+flavor=genome_purifyingonly+seed=1+ncycle=5000000+ext=.pqt",
    "${HOME}/scratch/2024-12-25/lex12+async-ga/wse-sketches+genome-flavor=genome_purifyingplus+seed=2/kernel-async-ga/a=fossils+flavor=genome_purifyingplus+seed=2+ncycle=5000000+ext=.pqt",
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

echo "phylo_source_path \${phylo_source_path}"

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
newick_outpath="\${MYLOCAL}/phylo.newick"
echo "genomes_inpath \${genomes_inpath}"
echo "phylo_outpath \${phylo_outpath}"
echo "newick_outpath \${newick_outpath}"

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

container="docker://ghcr.io/mmore500/hstrat@sha256:2bd2ce23aee4a3a1e41552500c84d33af413400eaf21ca18e9430f1554bfbd99"
echo "container \${container}"

export PYTHONUNBUFFERED=1
export SINGULARITYENV_PYTHONUNBUFFERED=1

echo "do work ----------------------------------------------------- \${SECONDS}"
echo "warmup jit cache"
warmup_outpath="/tmp/\$(uuidgen).pqt"
echo "/local/\$(basename "\${genomes_inpath}")" \
    | singularity exec \${container} \
        python3 -O -m hstrat.dataframe.surface_build_tree \
        "\${warmup_outpath}" \
        --head 100

stdbuf -o0 singularity exec \${container} \
        python3 -O -m hstrat._auxiliary_lib._alifestd_as_newick_asexual \
        -i  "\${warmup_outpath}" \
        -o "/tmp/\$(uuidgen).pqt"


echo "do reconstruction and postprocessing"
stdbuf -e0 -i0 -o0 echo "/local/\$(basename "\${genomes_inpath}")" \
    | stdbuf -o0 singularity exec \${container} \
        python3 -O -m hstrat.dataframe.surface_build_tree \
        "/local/\$(basename "\${phylo_outpath}")" \
        --exploded-slice-size 50_000_000 \
        --trie-postprocessor "hstrat.AssignOriginTimeNodeRankTriePostprocessor(t0='dstream_S')" \
        --shrink-dtypes --eager-write \
        --write-kwarg 'compression="lz4"' \
        --head "\${num_tips}" \
        --drop "genomeFlavor" \
        --drop "is_extant" \
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
du -h "\${newick_outpath}"
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
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --mail-user=mawni4ah2o@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --account=beacon
#SBATCH --requeue

${JOB_PREAMBLE}

echo "BATCHDIR ${BATCHDIR}"
ls -l "${BATCHDIR}"

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
# pushd "${BATCHDIR}/.."
#     tar czvf \
#     "${BATCHDIR_JOBRESULT}/a=jobarchive+date=${JOBDATE}+job=${JOBNAME}+ext=.tar.gz" \
#     "$(basename "${BATCHDIR}")"/__*
# popd

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
# cd "${BATCHDIR}"
# for f in _*; do
#     echo "tar and rm \$f"
#     tar cf "\${f}.tar" -h "\${f}"
#     rm -rf "\${f}"
# done
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

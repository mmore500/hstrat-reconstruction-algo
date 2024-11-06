#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "configuration ==========================================================="
JOBDATE="$(date '+%Y-%m-%d')"
echo "JOBDATE ${JOBDATE}"

JOBNAME="$(basename -s .sh "$0")"
echo "JOBNAME ${JOBNAME}"

JOBPROJECT="$(basename -s .git "$(git remote get-url origin)")"
echo "JOBPROJECT ${JOBPROJECT}"

HSTRAT_REVISION="469fdb71a01923629243d9828bbe69b022aa8ebe"
echo "HSTRAT_REVISION ${HSTRAT_REVISION}"
HSTRAT_REMOTE_URL="https://github.com/mmore500/hstrat.git"
echo "HSTRAT_REMOTE_URL ${HSTRAT_REMOTE_URL}"

SOURCE_REVISION="$(git rev-parse HEAD)"
echo "SOURCE_REVISION ${SOURCE_REVISION}"
SOURCE_REMOTE_URL="$(git config --get remote.origin.url)"
echo "SOURCE_REMOTE_URL ${SOURCE_REMOTE_URL}"

echo "initialization telemetry ==============================================="
echo "date $(date)"
echo "hostname $(hostname)"
echo "PWD ${PWD}"
echo "SLURM_JOB_ID ${SLURM_JOB_ID:-nojid}"
echo "SLURM_ARRAY_TASK_ID ${SLURM_ARRAY_TASK_ID:-notid}"
module purge || :
module load Python/3.10.8 || :
echo "python3.10 $(which python3.10)"
echo "python3.10 --version $(python3.10 --version)"

echo "setup HOME dirs ========================================================"
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

echo "setup BATCHDIR =========================================================="
BATCHDIR="${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/${JOBDATE}"
if [ -e "${BATCHDIR}" ]; then
    echo "BATCHDIR ${BATCHDIR} exists, clearing it"
fi
rm -rf "${BATCHDIR}"
mkdir -p "${BATCHDIR}"
echo "BATCHDIR ${BATCHDIR}"

ln -sf "${BATCHDIR}" "${HOME}/scratch/${JOBPROJECT}/${JOBNAME}/latest"

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
python3.10 -m venv --system-site-packages "${BATCHDIR_ENV}"
source "${BATCHDIR_ENV}/bin/activate"
echo "python3.10 $(which python3.10)"
echo "python3.10 --version $(python3.10 --version)"
for attempt in {1..5}; do
    python3.10 -m pip install --upgrade pip setuptools wheel || :
    python3.10 -m pip install --upgrade uv \
    && python3.10 -m uv pip install \
        'more_itertools==10.*' \
        'numpy==1.*' \
        'joinem==0.7.0' \
        'pandas==1.*' \
        'polars==1.6.*' \
        'pyarrow==15.*' \
        'scipy==1.*' \
        'tqdm==4.*' \
        "git+${HSTRAT_REMOTE_URL}@${HSTRAT_REVISION}" \
        'multiprocess==0.70.12.2' \
        'cppimport==21.*' \
    && break || echo "pip install attempt ${attempt} failed"
    if [ ${attempt} -eq 3 ]; then
        echo "pip install failed"
        exit 1
    fi
done

echo "setup dependencies ========================================== \${SECONDS}"
source "${BATCHDIR_ENV}/bin/activate"
python3.10 -m uv pip freeze

echo "sbatch preamble ========================================================="
JOB_PREAMBLE=$(cat << EOF
set -e
shopt -s globstar

# adapted from https://unix.stackexchange.com/a/504829
handlefail() {
    echo ">>>error<<<" || :
    awk 'NR>L-4 && NR<L+4 { printf "%-5d%3s%s\n",NR,(NR==L?">>>":""),\$0 }' L=\$1 \$0 || :
    ln -sf "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.failed" || :
    ln -sf "\${JOBLOG}" "\${HOME}/joblatest/joblog.failed" || :
    $(which scontrol || which echo) requeuehold "${SLURM_JOBID:-nojid}"
}
trap 'handlefail $LINENO' ERR

echo "initialization telemetry ------------------------------------ \${SECONDS}"
echo "HSTRAT_REVISION ${HSTRAT_REVISION}"
echo "SOURCE_REVISION ${SOURCE_REVISION}"
echo "BATCHDIR ${BATCHDIR}"

echo "cc SLURM script --------------------------------------------- \${SECONDS}"
JOBSCRIPT="\${HOME}/jobscript/\${SLURM_JOB_ID:-nojid}"
echo "JOBSCRIPT \${JOBSCRIPT}"
cp "\${0}" "\${JOBSCRIPT}"
chmod +x "\${JOBSCRIPT}"
cp "\${JOBSCRIPT}" "${BATCHDIR_JOBSCRIPT}/\${SLURM_JOB_ID:-nojid}"
ln -sf "\${JOBSCRIPT}" "${HOME}/joblatest/jobscript.launched"

echo "cc job log -------------------------------------------------- \${SECONDS}"
JOBLOG="\${HOME}/joblog/\${SLURM_JOB_ID:-nojid}"
echo "JOBLOG \${JOBLOG}"
touch "\${JOBLOG}"
ln -sf "\${JOBLOG}" "${BATCHDIR_JOBLOG}/\${SLURM_JOB_ID:-nojid}"
ln -sf "\${JOBLOG}" "\${HOME}/joblatest/joblog.launched"

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
module load Python/3.10.8 || :
echo "python3.10 \$(which python3.10)"
echo "python3.10 --version \$(python3.10 --version)"

echo "setup dependencies- ----------------------------------------- \${SECONDS}"
source "${BATCHDIR_ENV}/bin/activate"
python3.10 -m uv pip freeze

EOF
)

echo "create sbatch file: work ==============================================="

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# WORK ---------------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --constraint=amr
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --mail-user=ica7pzrtue@pomail.net
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --requeue
#SBATCH --array=0-5

${JOB_PREAMBLE}

echo "lscpu ------------------------------------------------------- \${SECONDS}"
lscpu || :

echo "lshw -------------------------------------------------------- \${SECONDS}"
lshw || :

echo "cpuinfo ----------------------------------------------------- \${SECONDS}"
cat /proc/cpuinfo || :

echo "do work ----------------------------------------------------- ${SECONDS}"
source "${BATCHDIR_ENV}/bin/activate"

python3 << EOF_
import pandas as pd
import time
from hstrat import hstrat

# pull sample trees from OSF
csv_files = [
    "https://osf.io/hkwj9/download",  # nk_ecoeaselection.csv
    "https://osf.io/zrhuf/download",  # nk_lexicaseselection.csv
    "https://osf.io/5uzh8/download"   # nk_tournamentselection.csv
]

# Function to load CSV files from URLs into DataFrames
def load_csv(file_url):
    print(f"Loading file from URL: {file_url}")
    df = pd.read_csv(file_url)
    print("Columns in CSV:", df.columns)
    return df

# Function to benchmark the build_tree_trie method
def benchmark_build_tree_trie(population, func_name, repetitions=10):
    records = []  # List to store benchmarking results

    # Loop to perform benchmarking with the specified number of repetitions
    for _ in range(repetitions):
        # start timing
        start_real_time = time.perf_counter()
        start_cpu_time = time.process_time()

        # Execute the build_tree_trie method with the given parameters
        tree = hstrat.build_tree_trie(
            population,
            force_common_ancestry=True  # Ensure common ancestry handling
        )

        # end timing
        end_real_time = time.perf_counter()
        end_cpu_time = time.process_time()

        # Store the timing results
        records.append({
            "Function": func_name,
            "Real Time (s)": end_real_time - start_real_time,
            "CPU Time (s)": end_cpu_time - start_cpu_time
        })

    return records

# main loop for benchmarking build_tree_trie without postprocessors
results = []

# creating fixed resolution policy, kept getting errors otherwise
default_policy = hstrat.fixed_resolution_algo.Policy(3)

# Iterate through the list of CSV file URLs
for file_url in csv_files:
    # Load population data from each CSV
    population_data = load_csv(file_url)

    # Create a list to represent the population
    population = []
    for index, row in population_data.iterrows():
        column = hstrat.HereditaryStratigraphicColumn(
            stratum_retention_policy=default_policy,
            stratum_differentia_bit_width=16
        )
        # Add strata based on depth information in the CSV
        column.DepositStrata(int(row['depth']))
        population.append(column)

    # Benchmark build_tree_trie without any postprocessing
    results += benchmark_build_tree_trie(
        population,
        func_name="build_tree_trie (no adjustment)"
    )

# convert the results to a DataFrame and save them as a CSV file
benchmark_df = pd.DataFrame(results)

outpath = "a=result+what=benchmark+ext=.csv"
print(f"writing {outpath}")
benchmark_df.to_csv(outpath, index=False)

EOF_







echo "finalization telemetry -------------------------------------- \${SECONDS}"
ls -l \${JOBDIR}
du -h \${JOBDIR}
ln -sf "\${JOBSCRIPT}" "${HOME}/joblatest/jobscript.finished"
ln -sf "\${JOBLOG}" "${HOME}/joblatest/joblog.finished"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'

EOF
###############################################################################
# --------------------------------------------------------------------------- #
###############################################################################


echo "submit sbatch file ====================================================="
$(which sbatch && echo --job-name="${JOBNAME}" || which bash) "${SBATCH_FILE}"

echo "create sbatch file: collate ============================================"

SBATCH_FILE="$(mktemp)"
echo "SBATCH_FILE ${SBATCH_FILE}"

###############################################################################
# COLLATE ------------------------------------------------------------------- #
###############################################################################
cat > "${SBATCH_FILE}" << EOF
#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output="/mnt/home/%u/joblog/%j"
#SBATCH --mail-user=ica7pzrtue@pomail.net
#SBATCH --mail-type=ALL
#SBATCH --requeue

${JOB_PREAMBLE}

echo "BATCHDIR ${BATCHDIR}"
ls -l "${BATCHDIR}"

echo "finalize ---------------------------------------------------- \${SECONDS}"
echo "   - archive job dir"
pushd "${BATCHDIR}/.."
    tar czvf \
    "${BATCHDIR_JOBRESULT}/a=jobarchive+date=${JOBDATE}+job=${JOBNAME}+ext=.tar.gz" \
    "$(basename "${BATCHDIR}")"/__*
popd

echo "   - join result"
ls -1 "${BATCHDIR}"/__*/**/a=result+* \
    | tee /dev/stderr \
    | python3.10 -m joinem --progress \
        "${BATCHDIR_JOBRESULT}/a=result+date=${JOBDATE}+job=${JOBNAME}+ext=.csv"
ls -l "${BATCHDIR_JOBRESULT}"
du -h "${BATCHDIR_JOBRESULT}"

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
cd "${BATCHDIR}"
for f in _*; do
    echo "tar and rm \$f"
    tar cf "\${f}.tar" -h "\${f}"
    rm -rf "\${f}"
done
cd
ls -l "${BATCHDIR}"

echo "finalization telemetry -------------------------------------- \${SECONDS}"
ln -sf "\${JOBSCRIPT}" "\${HOME}/joblatest/jobscript.completed"
ln -sf "\${JOBLOG}" "\${HOME}/joblatest/joblog.completed"
ln -sf "${BATCHDIR_JOBRESULT}" "\${HOME}/joblatest/jobresult.completed"
echo "SECONDS \${SECONDS}"
echo '>>>complete<<<'

EOF
###############################################################################
# --------------------------------------------------------------------------- #
###############################################################################

echo "submit sbatch file ====================================================="
$(which sbatch && echo --job-name="${JOBNAME}" --dependency=singleton || which bash) "${SBATCH_FILE}"

echo "finalization telemetry ================================================="
echo "BATCHDIR ${BATCHDIR}"
echo "SECONDS ${SECONDS}"
echo '>>>complete<<<'

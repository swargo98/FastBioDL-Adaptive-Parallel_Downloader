# SeqFlux

SeqFlux is a resource-aware pipeline for turning NCBI SRA accessions into analysis-ready compressed FASTQ files. It treats acquisition as an end-to-end systems problem: SRA archives are resolved from NCBI, downloaded over segmented HTTPS, converted with `fasterq-dump`, and compressed with `pigz` while the stages overlap through queues.

SRA acquisition is not just a download step. Archives expand substantially during FASTQ conversion, conversion stresses CPU and scratch storage, and compression adds another I/O-heavy stage. SeqFlux coordinates these stages so network, CPU, and storage resources can stay active without blindly overcommitting scratch space.

## What SeqFlux Does

- Resolves SRA accession IDs to NCBI download URLs with rate-limited E-utilities requests.
- Downloads `.sra` archives with resumable segmented HTTPS transfers.
- Adapts download worker concurrency online using a utility-guided gradient controller.
- Converts SRA archives to FASTQ with `fasterq-dump`.
- Runs conversion and compression concurrently with independent `fasterq-dump` and `pigz` process caps.
- Compresses FASTQ output with `pigz` and writes `.fastq.gz` files directly to the chosen output tier.
- Uses CPU, disk I/O, free-space, and phase-aware reservation checks before admitting conversion work.
- Records benchmark timing metadata for download, conversion, compression, and pairwise stage overlap.

## Requirements

- Python 3.9 or newer
- Python packages listed in `requirements.txt`
- NCBI SRA Toolkit, especially `fasterq-dump`
- `pigz`

Install Python dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Install SRA Toolkit and `pigz` with your platform package manager, or use the helper script:

```bash
source setup_sratools.sh --persist
```

## Run Locally

Create a text file with one accession per line:

```text
SRR000001
SRR000002
```

Run SeqFlux:

```bash
python seqflux.py \
  --input accessions.txt \
  --sra-dir /path/to/work/sra \
  --fastq-dir /path/to/work/fastq \
  --out-dir /path/to/final/fastq-gz
```

The output directory receives final `.fastq.gz` files. SeqFlux writes logs and timing JSON files under `logs/seqflux/<accession-list-name>/`.

## Storage Layout

SeqFlux separates the working tier from the final destination tier:

- `--sra-dir`: where downloaded `.sra` archives are written.
- `--fastq-dir`: scratch workspace for `fasterq-dump` output and temporary files.
- `--out-dir`: destination for compressed `.fastq.gz` files written by `pigz`.

On systems with multiple storage tiers, put `--sra-dir` and `--fastq-dir` on the tier best suited for high-throughput scratch work. The included Expanse configuration uses Lustre for SRA and FASTQ scratch, then writes compressed output to node-local NVMe.

If `--sra-dir` or `--fastq-dir` is omitted, SeqFlux creates a per-process scratch directory under `LOCAL_SCRATCH`, or under `/scratch/$USER/job_$SLURM_JOB_ID` when `LOCAL_SCRATCH` is not set.

## Configuration

Runtime defaults live in `config_seqflux.py`. Important knobs include:

- `thread_limit`: maximum active download workers.
- `method`: download optimizer, currently `gradient` or `bayes`.
- `K`: utility penalty for download concurrency growth.
- `probing_sec`: optimizer probing interval.
- `max_conversion_jobs`: concurrent `fasterq-dump` process cap.
- `max_pigz_jobs`: concurrent `pigz` process cap.
- `conversion_threads`: threads passed to each conversion job.
- `conversion_required_factor`, `conversion_reserve_factor`, `conversion_pigz_reserve_factor`: phase-aware disk reservation factors.
- `download_disk_safety_margin_gb` and `conversion_disk_safety_margin_gb`: free-space safety margins.
- `ncbi_lookup_rps`: NCBI URL lookup rate limit.

The default reservation factors use conservative conversion accounting: `12.0` for conversion runway and reservation, and `3.5` for compressed-output reservation.

## Run On SDSC Expanse

The Expanse helper runs the FFS storage placement for clustered systems: SRA and FASTQ scratch on Lustre, compressed output on node-local NVMe.

```bash
sbatch run_seqflux_expanse.sh accessions_large_PRJNA251383.txt
```

Useful environment overrides:

- `ACCESSION_LIST`: accession list path when not passing an argument.
- `REPEATS`: number of benchmark repetitions, default `3`.
- `SLEEP_BETWEEN`: seconds between repetitions, default `60`.
- `RESULTS_ROOT`: benchmark aggregation directory.
- `LOCAL_SCRATCH_OVERRIDE`: explicit local scratch directory.
- `EXPANSE_NVME_DEVICE`: device name used for `/proc/diskstats` admission telemetry.

## Example Accession Lists

The repository includes sample accession cohorts used for development and benchmark runs:

- `accessions_small_PRJNA916347.txt`
- `accessions_medium_PRJNA353374.txt`
- `accessions_large_PRJNA251383.txt`

## Repository Layout

```text
.
├── seqflux.py                # main pipeline entry point
├── config_seqflux.py         # runtime configuration
├── converter.py              # fasterq-dump and pigz conversion/compression stage
├── ncbi_lookup.py            # NCBI URL resolution helpers
├── search.py                 # online concurrency optimizers
├── storage_config.py         # scratch-path and device helpers
├── utils.py                  # shared filesystem utilities
├── run_seqflux_expanse.sh    # SDSC Expanse benchmark runner
├── setup_sratools.sh         # SRA Toolkit and pigz setup helper
├── requirements.txt          # Python dependencies
└── accessions_*.txt          # example accession cohorts
```

## Benchmark Outputs

Each run records a JSON timing file with:

- total wall-clock time;
- download, conversion, and compression phase windows;
- pairwise overlaps between download, conversion, and compression;
- accession list and tool metadata.

These files are written as `benchmark_seqflux_results_<timestamp>.json` under the run log directory.

## License

MIT. See `LICENSE`.

# SeqFlux

SeqFlux is a resource-aware pipeline for acquiring NCBI SRA datasets and producing compressed FASTQ output. It overlaps three stages:

1. Adaptive segmented HTTPS download of SRA archives.
2. SRA-to-FASTQ conversion with `fasterq-dump`.
3. Parallel FASTQ compression with `pigz`.

SeqFlux was previously named FastBioDL, so some internal filenames still use `fastbiodl`.

## Requirements

- Python 3.9 or newer
- Python packages in `requirements.txt`
- NCBI SRA Toolkit, especially `fasterq-dump`
- `pigz`

Install the system tools with your platform package manager, or use:

```bash
source setup_sratools.sh --persist
```

Install Python dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run Locally

Create a text file with one SRA accession per line, then run:

```bash
python fastbiodl_upgrade.py \
  -i accessions.txt \
  --sra-dir /path/to/fast/sra \
  --fastq-dir /path/to/fast/fastq-work \
  --out-dir /path/to/final/fastq-gz
```

The final directory receives `.fastq.gz` files. Logs and timing metadata are written under `logs/fastbiodl/`.

## Run On Expanse

```bash
sbatch run_fastbiodl_expanse.sh /path/to/accessions.txt
```

The script runs SeqFlux only.

## Repository Layout

```text
.
├── config_fastbiodl.py       # SeqFlux runtime configuration
├── converter.py              # fasterq-dump and pigz pipeline stage
├── fastbiodl_upgrade.py      # main SeqFlux entry point
├── ncbi_lookup.py            # NCBI URL resolution helpers
├── requirements.txt          # Python dependencies
├── run_fastbiodl_expanse.sh  # Expanse SeqFlux runner
├── search.py                 # online concurrency optimizer
├── setup_sratools.sh         # helper for SRA Toolkit and pigz
├── storage_config.py         # scratch-path helpers
└── utils.py                  # shared utilities
```

## License

MIT. See `LICENSE`.

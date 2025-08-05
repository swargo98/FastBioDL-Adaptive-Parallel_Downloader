# FastBioDL — Adaptive Parallel Downloader for Genomic Data

FastBioDL is an open-source, adaptive, **HTTP/FTP** downloader that maximises
throughput by *learning* the right number of concurrent streams at run time.
It reproduces all experiments from our PDSW 25 Submission:

> **Adaptive Parallel Downloader for Large Genomic Datasets**  
> Rasman M. Swargo *et al.*, PDSW/SC 25

---

## Quick start

| Scenario | You need | Steps |
|----------|----------|-------|
| **Reproduce paper results (easy)** | A free Google account | 1. Open **`fastbiodl-artifact.ipynb`** in Colab.<br>2. Click **Runtime ▸ Run all**. |
| **Run locally** | Linux / macOS, Python ≥ 3.9 | 1. `git clone https://github.com/swargo98/FastBioDL-Adaptive-Parallel_Downloader.git`<br>2. `cd FastBioDL-Adaptive-Parallel_Downloader`<br>3. `chmod +x setup.sh && ./setup.sh`<br>4. `source venv/bin/activate`<br>5. Edit **`config_fastbiodl.py`** if you need to change config.<br>6. Run&nbsp;`python fastbiodl.py -i accessions.txt` |
| **High‑speed network tests** | Two hosts connected by ≥ 10 Gb s⁻¹ **or an FTP server** | 1. Perform steps 1–4 above on both of the nodes.<br>2. On the **source** node create or enable an FTP server (e.g., **vsftpd**).<br>3. Edit **`config_apd.py`** to point to the FTP endpoint, dataset path, and optimizer parameters.<br>4. Run&nbsp;`python adaptive_parallel_downloader.py` |
<!-- | **Run locally** | Linux / macOS, Python ≥ 3.9 | 1. `git clone https://github.com/swargo98/FastBioDL-Adaptive-Parallel_Downloader.git`<br>2. `cd FastBioDL-Adaptive-Parallel_Downloader`<br>3. `chmod +x setup.sh && ./setup.sh`<br>4. `source venv/bin/activate`<br>5. Edit **`config_fastbiodl.py`** if you need to change paths, *k*, or output dirs.<br>6. Run&nbsp;`python fastbiodl.py -i accessions.txt` |
| **High-speed network tests** | Two hosts connected by ≥ 10 Gb s⁻¹ **plus an FTP server** | 1. Perform steps 1–4 above on the **sink** node.<br>2. On the **source** node create or enable an FTP server (e.g., **vsftpd**).<br>3. Edit **`config_apd.py`** to point to the FTP endpoint, dataset path, and optimizer parameters.<br>4. Run&nbsp;`python adaptive_parallel_downloader.py` | -->

> **Important:** *Always* copy the example config most similar to your setup and
> adjust paths, accession lists, and the penalty constant **k**.

---

## Repository layout

```
FastBioDL-Adaptive-Parallel_Downloader/
├── accessions.txt # Example SRA/ENA run list
├── adaptive_parallel_downloader.py
├── config_apd.py # Config for adaptive_parallel_downloader.py
├── config_fastbiodl.py # Config for fastbiodl.py
├── fastbiodl.py
├── fastbiodl_artifact.ipynb # Colab walkthrough / reproduction notebook
├── get_pip.py # Bootstraps pip on minimal systems
├── requirements.txt # Exact Python package versions
├── search.py # optimizer
├── setup.sh # Creates venv
└── utils.py # Helper functions
```

---

## Dependencies

* Python 3.9 – 3.12  
* `requests`, `numpy`, `pandas`, `tqdm`, `matplotlib`, `argparse`  
* (High-speed tests) `iperf3`, any FTP server, ≥ 10 Gb s⁻¹ NIC

Exact versions are pinned in `requirements.txt`.

---

## Reproducing paper figures

| Figure / Table | Script / notebook cell | Expected runtime |
|----------------|------------------------|------------------|
| Table 3 (public endpoints) | `fastbiodl_artifact.ipynb` Cell \[5\] | Depends on the dataset |
| Figure 5 | Cell \[10\] | Depends on the dataset |
| Figure 6 (Fabric) | `adaptive_parallel_downloader.py` | Depends on link speed |

---

## Citing

This section will be updated soon.
<!-- If you use FastBioDL in academic work, please cite:

```
@inproceedings{Swargo2025FastBioDL,
  title     = {Adaptive Parallel Downloader for Large Genomic Datasets},
  author    = {Rasman Mubtasim Swargo and Md Arifuzzaman and Engin Arslan},
  booktitle = {Proc. PDSW/SC},
  year      = {2025}
}
``` -->

---

## License

MIT — see `LICENSE` for details.

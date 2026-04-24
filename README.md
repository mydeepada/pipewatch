# pipewatch

A lightweight CLI tool to monitor and alert on data pipeline health metrics in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source kafka://localhost:9092 --topic my-pipeline
```

Set alert thresholds and get notified when metrics fall outside expected ranges:

```bash
pipewatch monitor --source ./pipeline.log --alert latency>500ms --alert error_rate>0.05
```

Run a quick health check and exit:

```bash
pipewatch check --source postgres://localhost/mydb --query "SELECT COUNT(*) FROM jobs WHERE status='failed'"
```

### Key Options

| Flag | Description |
|------|-------------|
| `--source` | Data source URI or file path |
| `--alert` | Alert rule in `metric>threshold` format |
| `--interval` | Polling interval in seconds (default: `10`) |
| `--output` | Output format: `text`, `json`, or `slack` |

---

## Requirements

- Python 3.8+
- Dependencies are installed automatically via pip

---

## License

This project is licensed under the [MIT License](LICENSE).
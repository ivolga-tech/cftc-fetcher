# PAJ fetcher

Download and convert data from PAJ.

## Code quality

See https://git.nomics.world/dbnomics-fetchers/documentation/wikis/code-style

## Usage

### Download

```bash
python3 download.py <source-dir>
```

### Convert

1st time, create a virtual env and install requirements with:

```bash
pip install -r requirements.txt
```

else:

```bash
python3 convert.py <source-dir> <target-dir>
```

Folders ../paj-source-data and ../paj-json-data must exist

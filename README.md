# Guangya-FastLink-PyFork

A strict rapid-transfer CLI for Guangya cloud-drive JSON manifests. It supports recursive export, importing a single JSON manifest, batch import, batch difference checks, resumable SQLite state, and retry/delta JSON output.

The CLI uses server-side instant reuse. A new transfer request succeeds only when Guangya returns `code=156`; when the server prepares a regular upload task, the client attempts to delete that task and records `not_reusable`. File downloads, OSS uploads, and transfers between cloud-drive providers are outside the tool's scope.

## Installation

Requires Python 3.11 or later.

```bash
git clone https://github.com/ImoutoHeaven/Guangya-FastLink-PyFork.git
cd Guangya-FastLink-PyFork
python -m pip install .
```

After installation, run:

```bash
guangya-fastlink --help
```

You can also run the module directly:

```bash
python -m guangya_fastlink --help
```

To create a project virtual environment with `uv`:

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv '.[test]'
```

## Credentials

Windows PowerShell:

```powershell
$env:GUANGYA_ACCESS_TOKEN = '...'
```

Linux, macOS, or Git Bash:

```bash
export GUANGYA_ACCESS_TOKEN='...'
```

Optional API origin:

```powershell
$env:GUANGYA_HOST = 'https://api.guangyapan.com'
```

```bash
export GUANGYA_HOST='https://api.guangyapan.com'
```

The access token is read from the environment and used only in the `Authorization: Bearer ...` request header. State databases, exported JSON, and ordinary logs do not store the token.

`import-json --dry-run` and `batch-import-json --dry-run` perform only local parsing, planning, and collision checks, so they do not require a token.

## JSON Format

Input files are compatible with the structure exported by the Guangya userscript. Each record requires `path`, `gcid`, and `size`:

```json
{
  "scriptVersion": "1.1.2",
  "scriptAuthor": "sumuve",
  "totalFilesCount": 1,
  "totalSize": 123,
  "formattedTotalSize": "123 B",
  "files": [
    {
      "size": "123",
      "path": "/dir/file.bin",
      "gcid": "58A3F526EE3C569FEECCDFD66DAC9631614E7578",
      "fileId": "123456789",
      "parentId": "987654321",
      "sourceGuangya": true
    }
  ],
  "sourceTag": "guangya"
}
```

Paths use `/` as the export root and are resolved relative to `--target-parent-id` during import. A GCID must be a 40-character hexadecimal string. `size` accepts a non-negative integer or a decimal-digit string, up to `2^63 - 1` bytes.

## Local GCID Generation

Recursively scan a local directory and generate Guangya JSON ready for import:

```bash
guangya-fastlink generate-json \
  --source-dir Movies \
  --output-file movies.json \
  --workers 8
```

The source checkout also supports running the same arguments directly:

```bash
python guangya_fastlink/local_calculator.py \
  --source-dir Movies \
  --output-file movies.json \
  --workers 8
```

This direct entry point uses only the Python standard library, so you can generate local GCID JSON before installing the project dependencies.

Output records contain:

```json
{
  "size": "209715200",
  "path": "/example.bin",
  "gcid": "82CD91242986D47ACC86ACC12D54C35A59381BA7"
}
```

The top-level `sourceTag` is `local`. If the output file is inside the source directory, the scan excludes it, allowing you to safely overwrite the previous output.

GCIDs use Xunlei-compatible, two-level block SHA-1 hashing: each file block produces a binary SHA-1 digest, and those digests are fed into an outer SHA-1 hash in file order. Block sizes are selected from 256 KiB, 512 KiB, 1 MiB, and 2 MiB according to the total file size.

The implementation uses the standard-library `hashlib` module. Python releases the GIL when hashing more than 2,047 bytes at once, allowing threads to run native hashing code in parallel for sufficiently large blocks. See the [Python hashlib documentation](https://docs.python.org/3.11/library/hashlib.html).

Workers are allocated automatically between files and blocks: many files are hashed in parallel at the file level, while a few large files are hashed in parallel at the block level. The read queue is bounded, and blocks are at most 2 MiB. Each worker establishes file identity from the opened file descriptor, allowing FUSE inode reassignment while files wait in the queue. File size and modification time must still match the scan, keeping record sizes and export totals consistent. After hashing, the descriptor and path must match the opened identity, size, and modification time, and the bytes read must match the expected size. A detected change fails the command and preserves any previous output. These checks validate individual reads; they do not provide a directory-wide snapshot or detect changes that leave all checked metadata unchanged.

The default worker count is the smaller of the logical CPU count and 8. Reduce `--workers` for mechanical disks; tune it against measured throughput for SSDs or NVMe drives.

## Directory IDs

The literal `root` in command arguments identifies the Guangya root directory, whose API `parentId` is an empty string:

```bash
--target-parent-id root
--source-parent-id root
```

For other directories, use the numeric directory ID from the web URL.

All remote directory reads share pagination integrity checks. Scanning starts at page 0, requires a consistent `total` across pages and unique `fileId` values, and verifies that the final count equals `total`. An early empty page, a duplicate across pages, or a changed `total` causes the command to fail; rerun once the directory is stable. A directory's scan results are cached or committed only after validation succeeds. Page-number pagination provides no server-side snapshot, so keep the source directory stable during scanning.

## Comparing Local and Remote Directories

Recursively compare file paths in a local directory and a Guangya directory, writing relative paths found only locally to standard output:

```bash
guangya-fastlink compare-folder \
  --local-folder /path/to/folder \
  --remote-folder-id 123456789 \
  > missing.txt
```

The default direction is equivalent to `--local-only`: files present locally but missing remotely. `--remote-only` selects files present remotely but missing locally. The two direction options are mutually exclusive:

```bash
guangya-fastlink compare-folder \
  --local-folder /path/to/folder \
  --remote-folder-id 123456789 \
  --remote-only
```

The command loads both sets of file paths into memory and prints the selected set difference in lexicographic order, with a leading `/`. Paths are compared relative to the specified directory on each side. Example `--local-only` output:

```text
/foldert/m2.txt
/missing_file1.txt
```

### Comparing Rapid-Transfer JSON with a Remote Directory by GCID

`--local-json` accepts rapid-transfer JSON generated by the local calculator or exporter. It checks content presence by GCID, including in remote directories where files have been flattened or renamed:

```bash
guangya-fastlink compare-folder \
  --local-json local.json \
  --remote-folder-id 123456789 \
  --output-file missing.json
```

`--local-json` and `--local-folder` are mutually exclusive. JSON mode requires a separate `--output-file`; directory mode writes a path list to standard output.

The default `--local-only` direction outputs local manifest records whose GCIDs are absent from the entire remote directory tree, preserving their original relative paths and supported optional fields. `--remote-only` outputs remote records whose GCIDs are absent from the local manifest, preserving remote relative paths, `fileId`, and `parentId`. As in the export format, an empty root `parentId` is omitted:

```bash
guangya-fastlink compare-folder \
  --local-json local.json \
  --remote-folder-id 123456789 \
  --remote-only \
  --output-file remote-only.json
```

The comparison key is the case-normalized GCID alone; paths, names, and sizes remain record data. Matching is based on content presence: one file with the same GCID on the other side matches every path containing that content. For entirely missing content, every original path record on the selected side is retained. Results are sorted by path and use the existing `files[].path/gcid/size` JSON format. File counts and total sizes are recalculated from the output records. The `sourceTag` is `local` for local-only output and `guangya` for remote-only output.

Both sides' records are loaded into memory, and every run rescans the entire remote directory tree. Regenerate the local JSON after local files change. The output is replaced atomically after scanning and validation succeed. An empty difference produces `files: []` and zero counts. Invalid input, remote request failures, incomplete pagination, or invalid GCIDs cause the command to fail while preserving the previous valid output.

The `files[].path` values in local-only results can be passed directly to link-generation scripts that support this field, using the original local paths to build HTTP(S) links for missing files. Remote-only paths are relative to the remote directory.

## Importing a Single JSON Manifest

Plan and validate a JSON manifest without changing remote content:

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id root \
  --state-file .state/export.import-state.sqlite3 \
  --dry-run
```

Run a strict rapid transfer:

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id 123456789 \
  --state-file .state/export.import-state.sqlite3 \
  --workers 5 \
  --max-retries 5 \
  --flush-every 100
```

Reuse the same state database to resume an interrupted import. Existing files with the same name are reconciled by `gcid + fileSize`: matching content is marked complete, while different content or an object-type collision is recorded as `failed`. This also handles interruptions after remote completion but before the local state is committed.

Requeue `failed` and `not_reusable` records:

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id 123456789 \
  --state-file .state/export.import-state.sqlite3 \
  --retry-failed
```

At the end of a run, failed and non-reusable records are written to a `*.retry.export.json` file beside the state database. The output uses the Guangya JSON format and can be imported again.

## Recursive Export

```bash
guangya-fastlink export-json \
  --source-parent-id 123456789 \
  --output-file export.json \
  --state-file .state/export.state.json \
  --workers 5 \
  --max-retries 5
```

The exporter recursively reads directories with `resType=2` and files with `resType=1`. Scan progress is stored in the state JSON and a `*.records.jsonl` sidecar. The final output is committed atomically through a temporary file, and internal state files are cleaned up after success.

Resuming an export requires state containing the current pagination-validation version. If that version is missing or incompatible, the command asks you to rescan with a new `--state-file`, preserving the original state and output files.

## Batch Import

```bash
guangya-fastlink batch-import-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/import \
  --workers 5 \
  --json-parallelism 2
```

The command recursively discovers `*.json` files and creates a separate SQLite state database for each manifest. All jobs complete local path-collision checks before any remote changes begin. `--dry-run` performs only discovery, parsing, planning, and collision checks.

The input and state directories must be disjoint: neither may equal, contain, or be contained by the other. A parsing failure in one JSON manifest counts as a batch failure while other successfully planned jobs continue. Target-path collisions across jobs abort the batch before any remote changes.

## Batch Difference Checks

Check by path:

```bash
guangya-fastlink batch-check-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/check \
  --output-dir delta \
  --exist-only
```

Check by path, GCID, and size:

```bash
guangya-fastlink batch-check-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/check \
  --output-dir delta \
  --with-checksum
```

Each job maintains separate check state. Missing files or files with mismatched content are written to `*.delta.export.json`. When there are no file differences, the corresponding old delta file is removed. Missing directories count toward the state summary and place the expected files beneath them in the delta output.

The state database stores the remote directory and file snapshot from the initial check. Switching between `--exist-only` and `--with-checksum` recalculates the delta from the same snapshot without rescanning the remote tree. The input, state, and output directories must be pairwise disjoint.

Check state uses schema v3. Opening a v2 database with a matching scope preserves the local manifest plan, clears remote scan results, upgrades the schema, and rescans to establish a pagination-validated snapshot. If a v3 scan fails, validated progress is preserved; subsequent runs resume unfinished work.

## Exit Status

- `0`: The command completed. Single-manifest and batch imports may still include `not_reusable` records.
- `1`: Startup validation, credentials, path collisions, remote requests, or file imports failed.

Strict rapid-transfer response handling:

- `code=156`: Transfer complete.
- `code=159`: A directory with the same name already exists; reuse the directory ID in the response.
- HTTP 401/403 or `code=117`: Invalid credentials.
- HTTP 429 and 5xx: Retryable.
- A regular upload task returned without `code=156`: Attempt to delete the task and record `not_reusable`.

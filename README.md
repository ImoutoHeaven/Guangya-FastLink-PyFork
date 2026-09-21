# Guangya-FastLink-PyFork

光鸭云盘 JSON 严格秒传命令行工具。项目提供递归导出、单文件导入、批量导入、批量差异检查、SQLite 中断续跑和 retry/delta JSON。

版本 0.1 使用服务端即时复用路径。单条记录只有在光鸭返回 `code=156` 时进入 `completed`；服务端准备普通上传任务时，客户端尝试清理该任务并记录为 `not_reusable`。文件内容下载、OSS 上传和跨网盘搬运位于当前版本范围之外。

## 安装

需要 Python 3.11 或更高版本。

```bash
git clone https://github.com/ImoutoHeaven/Guangya-FastLink-PyFork.git
cd Guangya-FastLink-PyFork
python -m pip install .
```

安装后可使用：

```bash
guangya-fastlink --help
```

也可以直接运行：

```bash
python -m guangya_fastlink --help
```

使用 `uv` 创建项目虚拟环境：

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv '.[test]'
```

## 登录凭证

Windows PowerShell：

```powershell
$env:GUANGYA_ACCESS_TOKEN = '...'
```

Linux、macOS 或 Git Bash：

```bash
export GUANGYA_ACCESS_TOKEN='...'
```

可选的接口源地址（origin）：

```powershell
$env:GUANGYA_HOST = 'https://api.guangyapan.com'
```

```bash
export GUANGYA_HOST='https://api.guangyapan.com'
```

Token 通过环境变量读取，并仅作为 `Authorization: Bearer ...` 请求头使用。状态数据库、导出 JSON 和普通日志均不保存 Token。

`import-json --dry-run` 和 `batch-import-json --dry-run` 只执行本地解析、规划与碰撞检查，因此无需设置 Token。

## JSON 格式

输入文件兼容光鸭 userscript 导出的结构。每条记录的必需字段是 `path`、`gcid` 和 `size`：

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

路径以 `/` 表示导出根，导入时相对于 `--target-parent-id` 解析。GCID 必须是 40 位十六进制字符串；size 可以是非负整数或十进制字符串。

## 本地 GCID 生成

递归扫描本地目录并生成可直接导入的光鸭 JSON：

```bash
guangya-fastlink generate-json \
  --source-dir Movies \
  --output-file movies.json \
  --workers 8
```

仓库源码也支持直接执行同一组参数：

```bash
python guangya_fastlink/local_calculator.py \
  --source-dir Movies \
  --output-file movies.json \
  --workers 8
```

该直接入口只使用 Python 标准库，可以在尚未安装项目依赖时生成本地 GCID JSON。

输出记录包含：

```json
{
  "size": "209715200",
  "path": "/example.bin",
  "gcid": "82CD91242986D47ACC86ACC12D54C35A59381BA7"
}
```

顶层 `sourceTag` 为 `local`。输出文件位于源目录内时，扫描会排除该输出文件，因此可以安全覆盖上一次生成结果。

GCID 使用迅雷兼容的分块双层 SHA-1：每个文件块先生成二进制 SHA-1 摘要，再按文件顺序把块摘要输入外层 SHA-1。分块大小根据文件总大小在 256 KiB、512 KiB、1 MiB 和 2 MiB 之间选择。

实现使用 Python 标准库 `hashlib`。每次内部哈希输入至少为一个文件块，Python 会在超过 2047 字节的哈希计算中释放 GIL，因此多线程能够并行执行原生哈希代码。[Python hashlib 文档](https://docs.python.org/3.11/library/hashlib.html)

worker 会在文件级和块级之间自动分配：大量文件并行计算多个文件，少量大文件并行计算文件块。读取队列有界，最大块为 2 MiB。计算前后会核对文件大小、修改时间和文件身份；计算过程中发生变化的文件会使命令失败，避免输出混合内容的 GCID。

默认 worker 数为 CPU 逻辑核心数与 8 的较小值；机械硬盘可降低 `--workers`，SSD/NVMe 可按实测吞吐调整。

## 目录 ID

命令参数中的字面量 `root` 表示光鸭根目录，其接口 `parentId` 为空字符串：

```bash
--target-parent-id root
--source-parent-id root
```

普通目录使用网页 URL 中的数字目录 ID。

## 本地与远端目录比较

递归比较本地目录与光鸭目录中的文件路径，并把仅存在于本地的相对路径写入标准输出：

```bash
guangya-fastlink compare-folder \
  --local-folder /path/to/folder \
  --remote-folder-id 123456789 \
  > missing.txt
```

默认方向与 `--local-only` 等价，输出本地存在而远端缺少的文件。`--remote-only` 输出远端存在而本地缺少的文件。两个方向参数互斥：

```bash
guangya-fastlink compare-folder \
  --local-folder /path/to/folder \
  --remote-folder-id 123456789 \
  --remote-only
```

命令将两端的文件路径载入内存，以 `/` 开头并按字典序输出所选方向的差集。比较键为相对于两端指定目录的文件路径。`--local-only` 输出示例：

```text
/foldert/m2.txt
/missing_file1.txt
```

## 单文件导入

规划并检查 JSON，不改变远端内容：

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id root \
  --state-file .state/export.import-state.sqlite3 \
  --dry-run
```

执行严格秒传：

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id 123456789 \
  --state-file .state/export.import-state.sqlite3 \
  --workers 5 \
  --max-retries 5 \
  --flush-every 100
```

同一个状态数据库支持中断续跑。已有同名文件按 `gcid + fileSize` 对账：内容一致时直接完成，内容不同或对象类型冲突时记录 `failed`。这也覆盖远端完成后本地尚未提交状态时发生的中断。

重新排队 `failed` 和 `not_reusable`：

```bash
guangya-fastlink import-json \
  --file export.json \
  --target-parent-id 123456789 \
  --state-file .state/export.import-state.sqlite3 \
  --retry-failed
```

运行结束后，失败和未命中记录写入与状态数据库相邻的 `*.retry.export.json`。输出沿用光鸭 JSON 格式，可以再次导入。

## 递归导出

```bash
guangya-fastlink export-json \
  --source-parent-id 123456789 \
  --output-file export.json \
  --state-file .state/export.state.json \
  --workers 5 \
  --max-retries 5
```

导出器递归读取 `resType=2` 的目录和 `resType=1` 的文件。扫描进度保存在状态 JSON 和 `records.jsonl` sidecar 中；最终输出通过临时文件原子提交，成功后清理内部状态文件。

## 批量导入

```bash
guangya-fastlink batch-import-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/import \
  --workers 5 \
  --json-parallelism 2
```

命令递归发现 `*.json`，为每个 JSON 建立独立 SQLite 状态数据库。所有任务先完成本地路径碰撞检查，再开始远端变更。`--dry-run` 只执行发现、解析、规划和碰撞检查。

输入目录与状态目录必须彼此独立，任一目录都不能是另一目录的祖先或子目录。单个 JSON 的解析失败会计入 batch 失败并继续执行其他已成功规划的任务；跨任务目标路径碰撞会在所有远端变更前终止 batch。

## 批量差异检查

按路径检查：

```bash
guangya-fastlink batch-check-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/check \
  --output-dir delta \
  --exist-only
```

按路径、GCID 和大小检查：

```bash
guangya-fastlink batch-check-json \
  --input-dir exports \
  --target-parent-id 123456789 \
  --state-dir .state/check \
  --output-dir delta \
  --with-checksum
```

每个任务维护独立 check state。缺失或内容不一致的文件写入 `*.delta.export.json`；没有文件差异时清理对应的旧 delta 文件。目录缺失计入状态摘要，并使其下的预期文件进入 delta。

状态数据库保存第一次检查得到的远端目录和文件快照。从 `--exist-only` 切换到 `--with-checksum`，或反向切换时，会从同一快照重新计算 delta，无需再次扫描远端。输入、状态和输出目录必须两两独立。

## 退出状态

- `0`：命令完成；单文件或批量导入可以包含 `not_reusable`。
- `1`：启动校验、凭证、路径碰撞、远端请求或文件导入出现失败。

严格秒传的业务分类：

- `code=156`：完成。
- `code=159`：同名目录已存在，并复用响应中的目录 ID。
- HTTP 401/403 或 `code=117`：凭证失效。
- HTTP 429 和 5xx：可重试。
- 返回普通上传任务且没有 `code=156`：尝试清理任务并记录 `not_reusable`。

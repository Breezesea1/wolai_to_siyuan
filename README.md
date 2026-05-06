# sync-notes

`sync-notes` 是一个用于将本地 Markdown 笔记迁移/同步到思源笔记的 Python 脚本项目。项目会扫描指定的 Markdown 根目录，处理页面、链接、附件、表格等内容，并通过思源本地 API 写入目标笔记本路径。

## 功能概览

- 扫描本地 Markdown 文件并构建页面清单。
- 规范化 Markdown 链接、图片资源和页面路径。
- 将内容导入到思源笔记指定笔记本与目标目录。
- 生成同步摘要日志，便于检查迁移结果。
- 包含测试用例，便于验证链接解析、路径处理和同步逻辑。

## 环境要求

- Python >= 3.14
- `uv`（推荐，用于依赖安装与运行）
- 本机可访问的思源笔记服务

项目依赖见 `pyproject.toml`，当前主要依赖：

- `requests`

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

如果不使用 `uv`，也可以用常规虚拟环境方式安装依赖：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install requests
```

### 2. 配置环境变量

脚本支持通过环境变量覆盖默认配置。下面把代码里实际读取到的变量一次列全：

| 变量名 | 作用 | 是否必需 | 默认值 / 说明 |
| --- | --- | --- | --- |
| `SIYUAN_URL` | 思源 API 地址 | 否 | `http://127.0.0.1:6806` |
| `SIYUAN_TOKEN` | 思源 API Token | 是 | 空字符串；同步时必须提供 |
| `WOLAI_ROOT` | 待同步的本地 Markdown 根目录 | 否 | 当前目录 `.` |
| `SIYUAN_TARGET_ROOT` | 导入到思源中的目标路径 | 否 | `/迁移/wolai` |
| `SIYUAN_NOTEBOOK_ID` | 目标思源笔记本 ID | 是 | 空字符串；同步时必须提供，也可用 `--notebook-id` 覆盖 |
| `SIYUAN_MAX_HPATH_SEGMENTS` | 目标路径层级截断配置，`0` 表示不截断 | 否 | `0` |
| `WOLAI_API_BASE` | wolai API 基础地址 | 否 | `https://openapi.wolai.com` |
| `WOLAI_TOKEN` | wolai API 访问令牌 | 条件必需 | 如果提供了它，就会优先用它访问 wolai API |
| `WOLAI_APP_ID` | wolai App ID | 条件必需 | 与 `WOLAI_APP_KEY` 配对使用，二选一地替代 `WOLAI_TOKEN` |
| `WOLAI_APP_KEY` | wolai App Secret | 条件必需 | 与 `WOLAI_APP_ID` 配对使用，二选一地替代 `WOLAI_TOKEN` |

说明：

- `SIYUAN_TOKEN` 和 `SIYUAN_NOTEBOOK_ID` 是执行同步时的硬性必填项。
- `WOLAI_TOKEN` 与 `WOLAI_APP_ID`/`WOLAI_APP_KEY` 是可选的；只有在需要基于 wolai API 做父级关系发现时才会用到。
- 如果不打算使用 wolai API 相关能力，可以不配置任何 `WOLAI_*` 变量。

PowerShell 示例：

```powershell
$env:SIYUAN_URL = "http://127.0.0.1:6806"
$env:SIYUAN_TOKEN = "你的思源 Token"
$env:WOLAI_ROOT = "C:\你的\Markdown\目录"
$env:SIYUAN_TARGET_ROOT = "/迁移/wolai"
$env:SIYUAN_NOTEBOOK_ID = "你的笔记本 ID"
```

> 建议优先使用环境变量配置敏感信息，不要把个人 Token 或本地私有路径提交到公共仓库。

### 3. 运行同步

```bash
uv run python main.py
```

或：

```bash
python main.py
```

## 测试

运行测试：

```bash
uv run pytest
```

如果未安装 `pytest`，请先安装测试依赖或在当前环境中执行：

```bash
pip install pytest
pytest
```

## 项目结构

```text
.
├── main.py                 # 同步脚本主逻辑
├── pyproject.toml          # 项目元数据与依赖
├── uv.lock                 # uv 锁定文件
├── tests/                  # 测试用例
├── .gitignore              # Git 忽略规则
└── README.md               # 项目说明文档
```

## 注意事项

- 第一次正式同步前，建议先备份源 Markdown 目录和思源数据。
- 请确认思源服务已启动，且 `SIYUAN_TOKEN`、`SIYUAN_NOTEBOOK_ID` 配置正确。
- `.omx/`、虚拟环境、缓存、日志和本地密钥文件已通过 `.gitignore` 忽略。
- 如果准备公开仓库，请先检查代码中是否存在个人 Token、本地绝对路径或其他敏感信息。

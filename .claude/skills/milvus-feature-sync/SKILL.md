---
name: milvus-feature-sync
description: 分析 pymilvus 文档与 CLI 代码的差距，引导实现缺失功能（Client/Command/Test 三层）
---

# Milvus Feature Sync

对比 pymilvus MilvusClient API 与 milvus_cli，找出缺失功能并实现（Client + CLI 命令 + 测试）。

## 核心原则

- **代码为准**: 从 `dir(MilvusClient)` 获取 API 列表，不依赖文档（文档可能版本不一致）
- **全面扫描**: CLI 命令可能通过 flag 合并实现（如 `create alias -A` = `alter_alias`），必须读代码确认
- **真实验证**: 测试必须连真实 Milvus，断言必须检查 output 内容（不能只查 exit_code）

## 执行步骤

### Step 0: 升级 pymilvus
```bash
./venv/bin/pip install --upgrade pymilvus
```

### Step 1: 从实际代码获取 API 列表
```bash
./venv/bin/python -c "
from pymilvus import MilvusClient
for m in sorted(dir(MilvusClient)):
    if not m.startswith('_'): print(m)
"
```
这是**唯一可信来源**。Context7 仅作补充参考（了解参数含义）。

### Step 2: 扫描 CLI 已实现命令

扫描 `milvus_cli/scripts/*_client_cli.py` 中所有 `@xxx.command()` 装饰器，**并读取实现代码**检查 flag 合并情况。

过滤掉：`__*__`, `_*`, `close`, `connect`, `create_schema`, `prepare_index_params`

### Step 3: 生成差距报告 → Step 4: 用户选择要实现的功能

### Step 5: 学习现有代码模式

读取同类别 1-2 个已有实现，学习三层模式：
- **Client**: `milvus_cli/*Client.py` — `_get_client()` + try/except 模式
- **CLI**: `milvus_cli/scripts/*_client_cli.py` — Click 装饰器 + `@click.pass_obj`
- **Test**: `tests/test_*.py` — `run_connected` fixture + assert code + assert output

### Step 6: 生成代码

三层同步生成，遵循学到的模式。别忘了更新 `milvus_cli/utils.py` 的自动补全。

### Step 7: 真实 Milvus 测试

向用户索要连接信息（MILVUS_URI + MILVUS_TOKEN），然后：
```bash
MILVUS_URI="<uri>" MILVUS_TOKEN="<token>" ./venv/bin/python -m pytest tests/test_xxx.py::TestXxx::test_method -v
```

如果测试通过但功能实际报错，用 CliRunner 快速验证：
```python
./venv/bin/python -c "
from click.testing import CliRunner
from milvus_cli.scripts.milvus_client_cli import cli
runner = CliRunner()
r = runner.invoke(cli, ['connect', '-uri', '<uri>', '-t', '<token>'])
r = runner.invoke(cli, ['<command>', '<args>'])
print(r.output, r.exit_code)
"
```

## 关键经验

| 踩坑 | 原因 | 解决 |
|------|------|------|
| API 不存在 | pymilvus 版本太旧 | Step 0 先升级 |
| 文档说有但实际没有 | Context7 对应不同版本 | 用 `dir()` 不用文档 |
| 漏判已实现功能 | flag 合并实现 | 读命令实现代码 |
| 测试假通过 | 错误输出到 stderr，exit_code 仍为 0 | 断言必须检查 output 内容 |
| 连接失败 | conftest 没传 token | 用 MILVUS_TOKEN 环境变量 |

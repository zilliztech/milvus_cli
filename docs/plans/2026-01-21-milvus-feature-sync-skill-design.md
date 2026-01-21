# Milvus Feature Sync Skill Design

**Date**: 2026-01-21
**Status**: Approved

## Overview

A Claude Code skill that analyzes pymilvus documentation, compares it with existing CLI code, identifies missing features, and guides implementation of new commands, tests, and help documentation.

## Requirements

| Requirement | Decision |
|-------------|----------|
| Goal | Interactive guided workflow |
| Doc source | Online real-time fetch via Context7 MCP |
| Comparison method | Feature category comparison |
| Code style | Auto-infer from existing code |
| Documentation | Built-in command help only (Click docstring) |
| API scope | MilvusClient only |
| Filtering | Auto-filter methods not suitable for CLI |

## Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    milvus-feature-sync                       │
├─────────────────────────────────────────────────────────────┤
│  1. 获取文档                                                 │
│     └─ 通过 Context7 获取 pymilvus MilvusClient 文档         │
│                                                              │
│  2. 分析现有代码                                             │
│     └─ 扫描 milvus_cli/ 下的 Client 类，提取已实现的功能     │
│                                                              │
│  3. 对比生成差距报告                                         │
│     ├─ 按类别分组（Collection, Index, Data, User 等）        │
│     ├─ 自动过滤不适合 CLI 的方法                             │
│     └─ 展示缺失功能列表                                      │
│                                                              │
│  4. 交互选择                                                 │
│     └─ 用户选择要实现的功能                                  │
│                                                              │
│  5. 引导实现                                                 │
│     ├─ 分析现有代码模式                                      │
│     ├─ 生成 Client 方法                                      │
│     ├─ 生成 CLI 命令                                         │
│     └─ 生成测试用例                                          │
└─────────────────────────────────────────────────────────────┘
```

## Auto-Filter Rules

| Category | Pattern | Reason |
|----------|---------|--------|
| Magic methods | `__*__` | Python internals |
| Private methods | `_*` | Internal implementation |
| Connection mgmt | `close`, `connect` | CLI handles separately |
| Context managers | Returns contextmanager | CLI uses commands instead |
| Callbacks | `*callback*`, `*listener*` | CLI is one-shot |
| Streaming | Returns iterator/generator | CLI prefers pagination |

## Feature Categories

- Database: database management
- Collection: collection management
- Index: index management
- Data: data operations (insert/search/query etc.)
- Partition: partition management
- User: user management
- Role: role and privilege management
- Alias: alias management
- Utility: utility methods (flush/compact etc.)

## Code Pattern Inference

### Client Layer
Analyze `milvus_cli/*Client.py`:
- Class structure and initialization
- Method signature style
- Interaction with self.client
- Error handling patterns
- Return value handling

### CLI Command Layer
Analyze `milvus_cli/scripts/*.py`:
- Click decorator usage
- Parameter definition style
- Interactive input handling
- Output formatting (OutputFormatter)
- Command grouping
- Docstring format for --help

### Test Layer
Analyze `tests/test_*.py`:
- Test function naming
- Fixture usage
- Assertion style
- Test data setup/teardown

## Implementation Modes

### Batch Mode
Generate all selected features at once, review together at the end.

### Individual Mode
Preview and confirm each feature one by one.

## Generated Artifacts

For each feature:
1. **Client method** in `milvus_cli/*Client.py`
2. **CLI command** in `milvus_cli/scripts/*.py` with full --help docstring
3. **Test case** in `tests/test_*.py`

## Skill Location

```
~/.claude/skills/milvus-feature-sync.md
```

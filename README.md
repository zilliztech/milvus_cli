# Milvus_CLI🚀

![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/zilliztech/milvus_cli?include_prereleases) ![PyPI](https://img.shields.io/pypi/v/milvus-cli)
![PyPI - Downloads](https://img.shields.io/pypi/dm/milvus_cli?label=PYPI%20downloads) ![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/downloads-pre/zilliztech/milvus_cli/latest/total?label=Release%40latest%20downloads) ![Docker Pulls](https://img.shields.io/docker/pulls/zilliz/milvus_cli)
![GitHub repo size](https://img.shields.io/github/repo-size/zilliztech/milvus_cli) ![PyPI - License](https://img.shields.io/pypi/l/milvus-cli)

## Overview

[Milvus](https://github.com/milvus-io/milvus) Command Line Interface based on [Milvus Python SDK](https://github.com/milvus-io/pymilvus).

- Applicable to most platforms: MS Windows, macOS, Ubuntu

- Support pip install & offline installation package

- Support single executable file

- Milvus Python SDK full function coverage

- Built-in help function

- Support auto completion

## Project Structure

```
milvus_cli/
├── Core Modules
│   ├── main.py          # Main entry point
│   ├── Cli.py           # CLI command interface
│   ├── Connection.py    # Milvus connection management
│   ├── Collection.py    # Collection operations
│   ├── Database.py      # Database management
│   ├── Index.py         # Index management
│   ├── Partition.py     # Partition management
│   ├── Data.py          # Data import/export
│   ├── Role.py          # Role management
│   ├── User.py          # User management
│   ├── Alias.py         # Alias management
│   ├── Fs.py            # File system operations
│   ├── Types.py         # Data type definitions
│   ├── utils.py         # Utility functions
│   └── Validation.py    # Input validation
├── scripts/             # CLI command implementations
│   ├── milvus_cli.py    # Main CLI script
│   ├── connection_cli.py # Connection-related commands
│   ├── collection_cli.py # Collection-related commands
│   ├── database_cli.py  # Database-related commands
│   ├── index_cli.py     # Index-related commands
│   ├── partition_cli.py # Partition-related commands
│   ├── data_cli.py      # Data-related commands
│   ├── role_cli.py      # Role-related commands
│   ├── user_cli.py      # User-related commands
│   ├── alias_cli.py     # Alias-related commands
│   └── helper_cli.py    # Helper commands
└── test/                # Unit tests
    ├── test_connection.py
    ├── test_collection.py
    ├── test_database.py
    ├── test_index.py
    ├── test_partition.py
    ├── test_data.py
    ├── test_role.py
    ├── test_user.py
    └── test_alias.py
```

### Core Components

- **Core Modules**: Implement the main functionality logic of Milvus CLI, each module handles specific Milvus feature domains
- **scripts/**: Contains all CLI command implementations, providing user interaction interfaces
- **test/**: Complete unit test suite ensuring code quality and functionality correctness

## Installation methods

### 🔝Install in a Python environment

#### Prerequisites

Python >= 3.8.5

#### Install from PyPI (Recommended)

Run `pip install pymilvus>=2.5.0`
Run `pip install milvus-cli==1.0.2`

#### Install from a tarball

1. Download the [latest release](https://github.com/zilliztech/milvus_cli/releases/latest) of ` milvus_cli-<version>.tar.gz`.
2. Run `pip install milvus_cli-<version>.tar.gz`.

#### Install from source code

1. Run `git clone https://github.com/zilliztech/milvus_cli.git`.
2. Run `cd milvus_cli`.
3. Run `pip install --editable .`

### Docker image in docker hub

`docker run -it zilliz/milvus_cli:latest`

## Usage

Run `milvus_cli` (in a Python environment) or double click `milvus_cli-<version>.exe` file (in a Windows environment).

#### Run Milvus_CLI

- In a Python environment, run `milvus_cli`.

### Document

https://milvus.io/docs/cli_commands.md

## Community

💬 Community isn’t just about writing code together. Come join the conversation, share your knowledge and get your questions answered on [Milvus Slack Channel](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ)!

<a href="https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ">
    <img src="https://assets.zilliz.com/readme_slack_4a07c4c92f.png" alt="Miluvs Slack Channel"  height="150" width="500">
</a>

<br><!-- Do not remove start of hero-bot --><br>
<img src="https://img.shields.io/badge/all--contributors-6-orange"><br>
<a href="https://github.com/chris-zilliz"><img src="https://avatars.githubusercontent.com/u/91247127?v=4" width="30px" /></a>
<a href="https://github.com/czhen-zilliz"><img src="https://avatars.githubusercontent.com/u/83751452?v=4" width="30px" /></a>
<a href="https://github.com/haorenfsa"><img src="https://avatars.githubusercontent.com/u/15938850?v=4" width="30px" /></a>
<a href="https://github.com/kateshaowanjou"><img src="https://avatars.githubusercontent.com/u/58837504?v=4" width="30px" /></a>
<a href="https://github.com/matrixji"><img src="https://avatars.githubusercontent.com/u/183388?v=4" width="30px" /></a>
<a href="https://github.com/sre-ci-robot"><img src="https://avatars.githubusercontent.com/u/56469371?v=4" width="30px" /></a>
<br><!-- Do not remove end of hero-bot --><br>

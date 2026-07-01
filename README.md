# ClusteredPilotExecutor

`ClusteredPilotExecutor` is a custom executor for the [Parsl](https://parsl-project.org/) workflow library. It is designed for executing scientific workflows in HPC environments managed by SLURM.

The executor receives ready-to-run tasks from Parsl, temporarily stores them in an internal queue, applies task clustering heuristics, and submits grouped tasks to SLURM through Parsl providers.

The main goal of the project is to explore task clustering and scheduling strategies for scientific workflows without replacing Parsl's workflow model.

This README explains how to set up the executor as a **standalone package** while enabling **import as if it were inside Parsl's `executors` folder** using a symlink-based development workflow.


## Overview

Parsl remains responsible for:

- defining Python and Bash apps;
- managing futures;
- resolving task dependencies;
- controlling the workflow execution graph;
- propagating results and exceptions.

`ClusteredPilotExecutor` is responsible for:

- receiving ready tasks from Parsl;
- grouping tasks before execution;
- applying clustering and scheduling heuristics;
- submitting pilot jobs through a SLURM provider;
- sending tasks to workers running inside the allocated job;
- returning task results to Parsl.

The executor is intended to work as an external Parsl-compatible package. It does not require modifying the Parsl source tree.

## Current Status

This project is a research prototype developed for experiments with scientific workflow execution in HPC environments.

The current implementation targets workflows composed mainly of `bash_app` tasks, especially bioinformatics pipelines that invoke external command-line tools.

## Requirements

- Python 3.8 or newer
- Parsl
- pyzmq
- pandas
- networkx
- matplotlib
- Access to an HPC environment with SLURM, when using `SlurmProvider`

## Installation

### 0. Install Parsl library in editable mode

```bash
git clone https://github.com/Parsl/parsl.git
cd parsl
pip install -e .
```
### 1. Clone the repository

```bash
git clone https://github.com/<username>/ClusteredParslExecutor.git
cd ClusteredParslExecutor
```

### 2. Install in editable mode

```bash
pip install -e .
```

### 2. Create and activate a Python environment

Using venv:

```
python -m venv .venv
source .venv/bin/activate
```
Using Conda:

```
conda create -n cpe python=3.10
conda activate cpe
```

### 3. Install Parsl

Install Parsl in the same Python environment:

```
pip install parsl
```

### 4. Install ClusteredPilotExecutor as an external package

For development installation:

```
pip install -e .
```

### 5. Import the executor

After installation, the executor should be imported directly from the external package:

```
from clustered_pilot_executor.executor import ClusteredPilotExecutor
from clustered_pilot_executor.executor import ClusteringAlgorithm
```

Do not import the executor from inside the Parsl namespace. Avoid imports such as:

from parsl.executors.clustered_pilot_executor.executor import ClusteredPilotExecutor

or:

from parsl.executors.adaptive_executor.executor import ClusteredPilotExecut

---
Alternatively, for development and testing purposes, you can create a symbolic link inside the Parsl executors directory. This allows the executor to be imported from the Parsl namespace.

To do this, go to the Parsl executors directory:

```
cd <parsl-root>/parsl/executors
```

Then create a symbolic link pointing to the cloned ClusteredPilotExecutor package:

```
ln -s /absolute/path/to/ClusteredPilotExecutor/clustered_pilot_executor clustered_pilot_executor
```

Replace ``/absolute/path/to/ClusteredPilotExecutor`` with the absolute path of the cloned repository.

After this, the executor can be imported using the Parsl namespace:

```
from parsl.executors.clustered_pilot_executor.executor import ClusteredPilotExecutor
from parsl.executors.clustered_pilot_executor.executor import ClusteringAlgorithm
```

This symlink-based installation is **not recommended** for regular use. It should be used only for local testing or development, because it depends on the internal layout of the Parsl installation and may break when Parsl is updated.

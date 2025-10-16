# AdaptivePilotExecutor

Executor for the Parsl library capable of submitting pilot jobs, clustering tasks according to different heuristics

This README explains how to set up the executor as a **standalone package** while enabling **import as if it were inside Parsl's `executors` folder** using a symlink-based development workflow.

---

## Installation

### 0. Install Parsl library in editable mode

```bash
git clone https://github.com/Parsl/parsl.git
cd parsl
pip install -e .
```
### 1. Clone the repository

```bash
git clone https://github.com/<username>/adaptive-parsl-executor.git
cd adaptive-parsl-executor
```

### 2. Install in editable mode

```bash
pip install -e .
```

### 3. Optional: Symlink into Parsl `executors` folder (development)

If you want to import the executor using the Parsl namespace (parsl.executors.adaptive_executor), create a symbolic link:

```bash
cd <parsl-root>/parsl/executors
ln -s /path/to/adaptive-parsl-executor adaptive_executor
```

Replace `/path/to/adaptive-parsl-executor` with the absolute path of the cloned repository. After this, you can import like:

```python
from parsl.executors.adaptive_executor.executor import AdaptivePilotExecutor
```

## Usage Example

```python
```

## Testing

``python
``

## Notes

* Designed for HPC bioinformatics pipelines using external tools.
* Fully compatible with any existing Parsl installation.w
* Threads are used for task orchestration; the GIL is irrelevant because the tasks spawn external processes.

## License

Include your preferred license here, e.g., MIT, Apache 2.0, or GPL.

## References

* Parsl: https://parsl-project.org

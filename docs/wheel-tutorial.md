# Python Wheel (.whl) Tutorial for Data Engineering

This guide covers how to create, update, and manage Python wheel files in this project, following data engineering and software development best practices.

---

## Table of Contents

1. [What is a Wheel File?](#what-is-a-wheel-file)
2. [Project Structure Overview](#project-structure-overview)
3. [Building Wheel Files](#building-wheel-files)
4. [Updating Wheel Files](#updating-wheel-files)
5. [Version Management Best Practices](#version-management-best-practices)
6. [CI/CD Integration](#cicd-integration)
7. [Databricks Bundle Integration](#databricks-bundle-integration)
8. [Troubleshooting](#troubleshooting)

---

## What is a Wheel File?

A **wheel** (`.whl`) is Python's built-package format that allows for faster installation compared to source distributions. Key benefits:

| Feature | Benefit |
| :--- | :--- |
| **Pre-compiled** | Faster installation, no build step needed |
| **Portable** | Easily distribute to Databricks clusters |
| **Versioned** | Track changes and rollback when needed |
| **Dependency bundled** | Include all your shared utilities |

### Wheel File Naming Convention

```
{project}-{version}-{python}-{abi}-{platform}.whl
```

Example: `dab_project-0.0.5-py3-none-any.whl`
- **dab_project**: Package name
- **0.0.5**: Version number
- **py3**: Python 3 compatible
- **none**: No ABI (Application Binary Interface) requirements
- **any**: Works on any platform

---

## Project Structure Overview

This project is set up for wheel-based distribution:

```
dab_project/
├── setup.py              # Package configuration
├── src/                  # Source code root
│   ├── citibike/         # Citibike-specific modules
│   ├── formula1/         # Formula 1-specific modules
│   ├── utils/            # Shared utilities
│   └── dab_project/      # Main package
├── dist/                 # Built wheel files
│   ├── dab_project-0.0.1-py3-none-any.whl
│   ├── dab_project-0.0.2-py3-none-any.whl
│   └── ...
└── build/                # Build artifacts (temporary)
```

### 📁 Project Examples

| File | Description |
| :--- | :--- |
| [setup.py](../setup.py) | Package configuration with version, name, and dependencies |
| [src/formula1/](../src/formula1/) | Example module with utilities and constants |
| [src/formula1/formula1_constants.py](../src/formula1/formula1_constants.py) | Constants module included in wheel |
| [dist/](../dist/) | Folder containing all built wheel versions |
| [.github/workflows/cd-workflow.yml](../.github/workflows/cd-workflow.yml) | CI/CD workflow with wheel dependencies |

---

## Building Wheel Files

### Prerequisites

Ensure you have the required build tools:

```bash
pip install --upgrade pip setuptools wheel build
```

### Method 1: Using `python -m build` (Recommended)

The modern, recommended approach:

```bash
# From the project root directory
python -m build --wheel

# Output will be in "dist/" folder
ls dist/
```

### Method 2: Using `setup.py` (Legacy)

```bash
# From the project root directory
python setup.py bdist_wheel

# Output will be in "dist/" folder
```

### Method 3: Using pip wheel

```bash
pip wheel . --wheel-dir dist/ --no-deps
```

> [!TIP]
> Use `python -m build --wheel` as it's the modern standard and handles build isolation automatically.

---

## Updating Wheel Files

### Step-by-Step Update Process

#### 1. Make Your Code Changes

Edit files in the `src/` directory:

```bash
# Example: Edit a utility file
vim src/formula1/formula1_utils.py
```

#### 2. Update the Version Number

Edit `setup.py` and increment the version:

```python
setup(
    name="dab_project",
    version="0.0.6",  # Increment from 0.0.5
    # ... rest of configuration
)
```

**Note**: Use `semver` practices. If your current version is `0.0.5`, update to `0.0.6` for patches.

#### 3. Clean Previous Build Artifacts

```bash
# Remove old build directories
rm -rf build/ dist/*.whl

# Or just the specific version if you want to keep history
rm dist/dab_project-0.0.5-py3-none-any.whl
```

#### 4. Build the New Wheel

```bash
python -m build --wheel
```

#### 5. Verify the Build

```bash
# Check the new wheel was created
ls -la dist/

# Inspect wheel contents
unzip -l dist/dab_project-0.0.6-py3-none-any.whl
```

> [!TIP]
> Always check the file list to ensure your new files (e.g., `src/new_module.py`) are actually included. If they are missing, check your `packages` discovery in `setup.py` or `MANIFEST.in`.

#### 6. Test the Wheel Locally

```bash
# Create a test environment
python -m venv .venv_test
source .venv_test/bin/activate

# Install the wheel
pip install dist/dab_project-0.0.6-py3-none-any.whl

# Test imports
python -c "from formula1 import formula1_utils; print('Success!')"

# Cleanup
deactivate
rm -rf .venv_test
```

---

## Version Management Best Practices

### Semantic Versioning (SemVer)

Follow [Semantic Versioning 2.0.0](https://semver.org/):

```
MAJOR.MINOR.PATCH
```

| Version Part | When to Increment | Example |
| :--- | :--- | :--- |
| **MAJOR** | Breaking API changes | `1.0.0` → `2.0.0` |
| **MINOR** | New features (backward compatible) | `1.0.0` → `1.1.0` |
| **PATCH** | Bug fixes (backward compatible) | `1.0.0` → `1.0.1` |

### Pre-release Versions

For development/testing:

```python
version="0.0.6.dev1"     # Development release
version="0.0.6a1"        # Alpha release
version="0.0.6b1"        # Beta release
version="0.0.6rc1"       # Release candidate
```

### Version History

Keep a `CHANGELOG.md` to track changes:

```markdown
## [0.0.6] - 2026-01-24
### Added
- New bronze layer ingestion notebooks
- Landing folder path constant

### Changed
- Updated schema validation logic

## [0.0.5] - 2026-01-20
### Added
- Formula 1 silver layer utilities
```

> [!IMPORTANT]
> Always increment the version before building a new wheel to avoid conflicts and enable proper rollbacks.

### 📁 Version Examples in This Project

- **Current version**: See [setup.py](../setup.py) line 5 (`version="0.0.5"`)
- **Built versions**: Check [dist/](../dist/) folder for all `.whl` files (0.0.1 through 0.0.5)

---

## CI/CD Integration

### Automatic Wheel Building in GitHub Actions

The project's CI/CD workflow already handles wheel building during deployment. Here's what happens:

```yaml
# From .github/workflows/cd-workflow.yml
- name: Install Dependencies
  run: |
    pip install --upgrade pip
    pip install setuptools wheel
```

### Enhanced CI Workflow with Wheel Build

For more control, consider adding a dedicated build step:

```yaml
- name: Build Wheel
  run: |
    pip install build
    python -m build --wheel

- name: Cache Wheel
  uses: actions/cache@v3
  with:
    path: dist/
    key: wheel-${{ hashFiles('setup.py', 'src/**') }}
```

### Databricks Asset Bundles Auto-Build

> [!NOTE]
> When using `databricks bundle deploy`, the bundle automatically builds wheels defined in the artifacts section of your bundle configuration. You don't need to manually build wheels for deployment.

---

## Databricks Bundle Integration

### How Bundles Handle Wheels

Databricks Asset Bundles can automatically:
1. Build wheels from your `setup.py`
2. Upload them to your workspace
3. Install them on cluster initialization

### Adding Wheel Artifacts to databricks.yml

```yaml
# Add to your databricks.yml
artifacts:
  default:
    type: whl
    path: .

# The wheel will be available in pipelines and jobs
```

### Using the Wheel in Notebooks

After deployment, use the wheel in notebooks:

```python
# The wheel is automatically available
from formula1.formula1_utils import parse_race_data
from utils.spark_utils import get_spark_session
```

### Cluster Library Configuration

For jobs, reference the wheel:

```yaml
# In resources/jobs/your_job.yml
libraries:
  - whl: ../../dist/*.whl
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. Module Not Found After Installation

**Problem**: `ModuleNotFoundError: No module named 'formula1'`

**Solution**: Check `packages` in `setup.py`:

```python
setup(
    packages=find_packages(where="./src"),  # Ensure this finds all packages
    package_dir={"": "./src"},
)
```

Verify packages are found:

```python
from setuptools import find_packages
print(find_packages(where="./src"))
# Expected: ['citibike', 'formula1', 'utils', 'dab_project']
```

#### 2. Old Version Still Installed

**Problem**: Changes not reflected after installing new wheel

**Solution**: Force reinstall:

```bash
pip install --force-reinstall dist/dab_project-0.0.6-py3-none-any.whl
```

#### 3. Missing Files in Wheel

**Problem**: Some files not included in the wheel

**Solution**: Ensure proper `__init__.py` files:

```
src/
├── formula1/
│   ├── __init__.py    # Required!
│   └── formula1_utils.py
```

For non-Python files, add `MANIFEST.in`:

```
include src/formula1/*.json
recursive-include src/utils/templates *
```

#### 4. Version Conflict

**Problem**: `pip` installs old version due to caching

**Solution**: Use specific version or clear cache:

```bash
pip cache purge
pip install dab_project==0.0.6
```

---

## Quick Reference Commands

| Task | Command |
| :--- | :--- |
| Build wheel | `python -m build --wheel` |
| List wheel contents | `unzip -l dist/*.whl` |
| Install wheel | `pip install dist/dab_project-X.X.X-py3-none-any.whl` |
| Force reinstall | `pip install --force-reinstall dist/*.whl` |
| Check installed version | `pip show dab_project` |
| Uninstall | `pip uninstall dab_project` |
| Clean build | `rm -rf build/ dist/*.whl *.egg-info` |

---

## Additional Resources

- [Python Packaging User Guide](https://packaging.python.org/)
- [PEP 427 - The Wheel Binary Package Format](https://peps.python.org/pep-0427/)
- [Databricks Asset Bundles - Python Wheel Jobs](https://docs.databricks.com/en/dev-tools/bundles/python-wheel.html)
- [Semantic Versioning](https://semver.org/)

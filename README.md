# The Yellow Sign - MLOps

## About

## Quick Start (dev mode)
### Step 1 - UV sync 
```bash
$env:Path = "C:\Users\danie\.local\bin;$env:Path"
```

```bash
uv sync
```

### Step 2 - PyCharm Interpreter Setup _(pycharm version 2025.02 example)_

1. Open **File → Settings → Python → Interpreter**
2. Click **Add Interpreter → Add Local Interpreter**
3. Under **Environment**, select **Use existing environment**
4. Set **Type** to **uv**
5. In **Path to uv** specify uv location (usually `/home/<your-user>/.local/bin/uv`)
6. In **Uv env use**, specify the Python executable from your project’s virtual environment (`/home/<path-to-mlops-repo>/mlops/.venv/bin/python`)
7. Confirm and apply the settings.

After that, PyCharm will use the same `uv` environment as your project.

### Step 3 - Set Up Pre-commit
1. Install pre-commit with uv: 
   ```bash
   uv tool install pre-commit --with pre-commit-uv
   ```
2. Verify the installation:
   ```bash
   pre-commit --version
   ```
3. Install the project hooks:
   ```bash
   pre-commit install --hook-type commit-msg
   ```
   
4. Test the setup by trying to make a non-conventional commit.
   The commit should fail and display an error message:
   ```bash
   git commit -m "fix workflow error"
   # > Conventional Commit......................................................Failed
   # ...
   ```
   
5. If the above test works as expected, pre-commit is successfully configured.

### Step 4 - Verify Ruff Setup
1. Run some Ruff check: 
   ```bash
   uv run ruff check .
   ```

2. If no errors are reported, Ruff is installed and working correctly.

## Contribute

# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %pip install ansi2html

# COMMAND ----------

import pytest
import os
import sys
from io import StringIO
import re
 	



repo_name = "pytest_poc"

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace/{repo_root}/{repo_name}")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

#original_stdout = sys.stdout
#captured_output = StringIO()
#sys.stdout = captured_output

pytest.main([".", "-v", "-p", "no:cacheprovider"])

#sys.stdout = original_stdout
#pytest_output = captured_output.getvalue()

# Remove ANSI escape codes using regular expressions
#ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
#cleaned_output = ansi_escape.sub('', pytest_output)'''




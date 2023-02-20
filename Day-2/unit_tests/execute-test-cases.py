# Databricks notebook source
# MAGIC %md
# MAGIC #### Install the pytest utility

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Execute the pytest feature for the unit tests covered

# COMMAND ----------

import pytest
import os
import sys

repo_name = "Databricks-training-repository"

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

print(notebook_path)

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

print(repo_root)

# Prepare to run pytest from the repo.
os.chdir(f"{repo_root}/{repo_name}")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------



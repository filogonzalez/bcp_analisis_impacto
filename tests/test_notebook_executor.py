# Databricks notebook source
%pip install pytest openpyxl
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
%sh
cd /Workspace/Users/filo.gzz@databricks.com/.bundle/bcp_analisis_impacto/dev/files/

# prevent the read-only /Workspace mount from trying to hold __pycache__
PYTHONDONTWRITEBYTECODE=1 \
python -m pytest -q tests/test_notebook_reader.py

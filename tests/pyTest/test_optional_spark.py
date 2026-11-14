import subprocess
import sys


def test_package_imports_without_pyspark():
    script = """
import builtins

original_import = builtins.__import__

def import_without_pyspark(name, *args, **kwargs):
    if name == "pyspark" or name.startswith("pyspark."):
        raise ModuleNotFoundError("No module named 'pyspark'", name="pyspark")
    return original_import(name, *args, **kwargs)

builtins.__import__ = import_without_pyspark

from BasketIntelligence.load_season_data import LoadSeasonData
from BasketIntelligence.utils.create_spark_session import get_spark

assert LoadSeasonData
try:
    get_spark()
except ModuleNotFoundError as error:
    assert error.name == "pyspark"
else:
    raise AssertionError("get_spark() should require the optional PySpark dependency")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

import os
import shutil
import sys
from pathlib import Path

from recovery.errors import RecoveryError

ROOT = Path(__file__).resolve().parents[1]


def configure_runtime():
    local_java = ROOT / ".tools" / "java-home.txt"
    if not os.environ.get("JAVA_HOME") and local_java.is_file():
        os.environ["JAVA_HOME"] = local_java.read_text(encoding="utf-8").strip()
    java_home = os.environ.get("JAVA_HOME")
    java = (
        Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java") if java_home else None
    )
    if not (java and java.is_file()) and not shutil.which("java"):
        raise RecoveryError(
            "java_missing", "Install Java 17+ and set JAVA_HOME before running Spark."
        )
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def create_spark():
    configure_runtime()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ETL Pipeline Recovery")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

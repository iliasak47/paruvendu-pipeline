import os
import shutil
import boto3
import logging
import tempfile
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------------------
# CONFIGURATION DU LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CREATION DE LA SESSION SPARK
# ---------------------------------------------------------------------
def create_spark_session(app_name="historize_paruvendu"):
    """Crée une session Spark locale pour l’historisation."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    return spark

# ---------------------------------------------------------------------
# UTILITAIRES S3
# ---------------------------------------------------------------------
def list_s3_files(bucket, prefix="gold/", suffix="_clean.parquet"):
    """
    Liste les fichiers S3 dans un bucket donné correspondant aux fichiers Gold.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                files.append(key)

    if not files:
        raise FileNotFoundError(f"Aucun fichier Gold trouvé sous s3://{bucket}/{prefix}")

    logger.info(f"{len(files)} fichiers Gold détectés sous s3://{bucket}/{prefix}")
    return files


def download_s3_to_temp(bucket, key):
    """
    Télécharge un fichier S3 vers un fichier temporaire local.
    Retourne le chemin local du fichier temporaire.
    """
    s3 = boto3.client("s3")
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
    s3.download_file(bucket, key, tmp_file)
    logger.info(f"Téléchargé : s3://{bucket}/{key} → {tmp_file}")
    return tmp_file


def upload_to_s3(local_path, bucket, key):
    """
    Upload un fichier Parquet vers S3.
    """
    s3 = boto3.client("s3")
    logger.info(f"Upload : {local_path} → s3://{bucket}/{key}")
    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, bucket, key)
    logger.info(f"Fichier uploadé avec succès vers s3://{bucket}/{key}")

# ---------------------------------------------------------------------
# CONSTRUCTION DE L’HISTORIQUE
# ---------------------------------------------------------------------

def build_history(spark, local_gold_files):
    """
    Concatène tous les fichiers Gold et construit un historique complet
    sans suppression de doublons, même identiques.
    Chaque ligne des fichiers sources est conservée telle quelle.
    """
    # Lecture de tous les fichiers Parquet Gold
    df = spark.read.parquet(*local_gold_files)

    # Conversion tolérante de la colonne 'date_scraping' en type Date
    # (certains fichiers peuvent avoir des formats différents)
    df = df.withColumn(
        "date_scraping",
        coalesce(
            expr("to_date(date_scraping, 'yyyy-MM-dd')"),
            expr("to_date(date_scraping, 'dd-MM-yyyy')")
        )
    )

    # --- Aucune suppression de doublons ---

    # Statistiques informatives pour le log
    total_count = df.count()
    unique_ids = df.select("id_annonce").distinct().count()
    unique_dates = df.select("date_scraping").distinct().count()

    logger.info(f"Total de lignes historisées (doublons inclus) : {total_count}")
    logger.info(f"Nombre d'annonces uniques : {unique_ids}")
    logger.info(f"Nombre de dates distinctes : {unique_dates}")

    df.groupBy("date_scraping").count().orderBy("date_scraping").show(truncate=False)

    return df



# ---------------------------------------------------------------------
# SAUVEGARDE DE L’HISTORIQUE
# ---------------------------------------------------------------------

def save_history(df, output_bucket, output_key):
    """Sauvegarde l'historique combiné en Parquet (un seul fichier) et l'upload vers S3."""
    import tempfile
    import shutil

    # Répertoire temporaire local
    tmp_output_dir = tempfile.mkdtemp()
    local_parquet_dir = os.path.join(tmp_output_dir, "history_parquet")

    # Force Spark à écrire un seul fichier Parquet
    df.coalesce(1).write.mode("overwrite").parquet(local_parquet_dir)
    logger.info(f"Historique sauvegardé temporairement : {local_parquet_dir}")

    # Trouve le vrai fichier .parquet à uploader
    parquet_files = [f for f in os.listdir(local_parquet_dir) if f.endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {local_parquet_dir}")
    file_to_upload = os.path.join(local_parquet_dir, parquet_files[0])
    logger.info(f"Fichier Parquet à uploader : {file_to_upload}")

    # Upload vers S3
    s3 = boto3.client("s3")
    logger.info(f"Upload : {file_to_upload} → s3://{output_bucket}/{output_key}")
    with open(file_to_upload, "rb") as f:
        s3.upload_fileobj(f, output_bucket, output_key)
    logger.info(f"Fichier uploadé avec succès vers s3://{output_bucket}/{output_key}")

    # Nettoyage local
    shutil.rmtree(tmp_output_dir, ignore_errors=True)
    logger.info("Fichiers temporaires supprimés après upload.")

# ---------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------
def main():
    """
    Pipeline complet d’historisation :
    1. Télécharge tous les fichiers Gold du bucket S3
    2. Combine et nettoie les données dans Spark
    3. Sauvegarde l’historique combiné dans S3
    """
    input_bucket = "paruvendu-data-lake"
    gold_prefix = "gold/"
    output_bucket = "paruvendu-data-lake"
    output_key = "history/history.parquet"

    spark = create_spark_session()

    try:
        # Étape 1 : Lister et télécharger tous les fichiers Gold
        gold_keys = list_s3_files(input_bucket, gold_prefix)
        local_gold_files = [download_s3_to_temp(input_bucket, k) for k in gold_keys]

        # Étape 2 : Construire l’historique complet
        df_history = build_history(spark, local_gold_files)

        # Étape 3 : Sauvegarder dans S3
        save_history(df_history, output_bucket, output_key)

        logger.info("Historisation terminée avec succès.")

    except Exception as e:
        logger.error(f"Erreur pendant l'historisation : {e}")
        raise

    finally:
        # Nettoyage des fichiers Gold téléchargés
        for f in locals().get("local_gold_files", []):
            try:
                os.remove(f)
            except Exception:
                pass

        spark.stop()
        logger.info("Session Spark arrêtée.")

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()

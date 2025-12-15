import os
import re
import boto3
import logging
import tempfile
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------------------------
# CONFIGURATION DU LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# OUTIL D'AFFICHAGE DE DIAGNOSTIC
# ---------------------------------------------------------------------
def show_df_info(df, title="DataFrame"):
    """
    Affiche des informations détaillées sur un DataFrame PySpark :
    - schéma
    - nombre de lignes
    - aperçu des premières lignes
    - nombre de valeurs nulles par colonne
    """
    print("\n" + "="*100)
    print(f"{title}")
    print("="*100)
    df.printSchema()
    print(f"Nombre de lignes : {df.count()}")
    df.show(10, truncate=False)
    print("Valeurs nulles par colonne :")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
    print("="*100 + "\n")

# ---------------------------------------------------------------------
# CREATION DE LA SESSION SPARK
# ---------------------------------------------------------------------
def create_spark_session(app_name="transform_paruvendu_local_debug"):
    """
    Crée une session Spark locale avec des paramètres optimisés pour le développement.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    return spark

# ---------------------------------------------------------------------
# FONCTIONS UTILITAIRES POUR S3
# ---------------------------------------------------------------------
def download_from_s3(bucket, key, local_path):
    """
    Télécharge un fichier S3 vers un fichier temporaire local.
    """
    s3 = boto3.client("s3")
    logger.info(f"Téléchargement depuis s3://{bucket}/{key} vers {local_path}")
    s3.download_file(bucket, key, local_path)

def upload_to_s3(local_path, bucket, key):
    """
    Upload un fichier (ou un dossier contenant un Parquet) vers S3.
    Si le chemin local est un dossier, prend le premier fichier .parquet à l'intérieur.
    """
    s3 = boto3.client("s3")

    # Si Spark a écrit un dossier Parquet, on récupère le vrai fichier .parquet
    if os.path.isdir(local_path):
        files = [os.path.join(local_path, f) for f in os.listdir(local_path) if f.endswith(".parquet")]
        if not files:
            raise FileNotFoundError(f"Aucun fichier .parquet trouvé dans {local_path}")
        file_to_upload = files[0]
    else:
        file_to_upload = local_path

    logger.info(f"Upload vers s3://{bucket}/{key} à partir de {file_to_upload}")

    with open(file_to_upload, "rb") as f:
        s3.upload_fileobj(f, bucket, key)

    logger.info(f"Fichier uploadé avec succès vers s3://{bucket}/{key}")

# ---------------------------------------------------------------------
# FONCTION DE TRANSFORMATION
# ---------------------------------------------------------------------
def clean_and_transform(df, input_path):
    """
    Nettoie et enrichit les données Silver pour produire le jeu de données Gold :
    - extraction du prix, surface, nb de pièces
    - ajout du prix/m² et de la ville
    - nettoyage du texte et création d'un identifiant unique
    """
    filename = os.path.basename(input_path)
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    date_scraping = match.group(1) if match else datetime.now().strftime("%Y-%m-%d")

    show_df_info(df, "Avant nettoyage (lecture brute)")

    # Extraction du prix
    df = df.withColumn(
        "price_eur",
        regexp_extract(col("price"), r"(\d+)\s*€", 1).cast(IntegerType())
    )
    show_df_info(df, "Après extraction du prix")

    # Extraction de la surface en m²
    df = df.withColumn(
        "surface_m2",
        regexp_extract(col("details"), r"(\d+)\s*m²", 1).cast(IntegerType())
    )
    show_df_info(df, "Après extraction surface")

    # Extraction du nombre de pièces à partir du titre
    df = df.withColumn(
        "nb_pieces",
        regexp_extract(lower(col("title")), r"(\d+)\s*pièce", 1).cast(IntegerType())
    )
    show_df_info(df, "Après extraction nb_pieces")

    # Extraction de la ville (ou Rouen par défaut)
    df = df.withColumn(
        "city",
        when(
            regexp_extract(col("details"), r"([A-ZÉÈÎÔÂÛÙÇ][a-zéèîôâûùç\- ]+)\s*\(76\)", 1) != "",
            regexp_extract(col("details"), r"([A-ZÉÈÎÔÂÛÙÇ][a-zéèîôâûùç\- ]+)\s*\(76\)", 1)
        ).otherwise(lit("Rouen"))
    )
    show_df_info(df, "Après extraction de la ville")

    # Filtrer uniquement les lignes avec des URLs valides
    df = df.filter(col("url").rlike("^https?://"))

    # Création d'un identifiant unique à partir de l'URL
    df = df.withColumn(
        "id_annonce",
        regexp_extract(regexp_replace(col("url"), r"/$", ""), r"([^/]+)$", 1)
    )

    # Ajout de la date de scraping (extraite du nom du fichier)
    df = df.withColumn("date_scraping", lit(date_scraping))
    show_df_info(df, "Après ajout id_annonce et date_scraping")

    # Calcul du prix au m²
    df = df.withColumn(
        "price_m2",
        when(
            (col("price_eur").isNotNull()) &
            (col("surface_m2").isNotNull()) &
            (col("surface_m2") > 0),
            round(col("price_eur") / col("surface_m2"), 2)
        ).otherwise(None)
    )
    show_df_info(df, "Après calcul du prix_m2")

    # Nettoyage du texte : suppression des retours à la ligne et espaces inutiles
    df = df.withColumn("description", regexp_replace(col("description"), r"[\r\n]+", " "))
    df = df.withColumn("title", trim(col("title")))

    # Sélection finale des colonnes pertinentes et suppression des doublons
    df = df.select(
        "id_annonce", "title", "price_eur", "surface_m2",
        "nb_pieces", "price_m2", "city", "description",
        "url", "date_scraping"
    ).dropDuplicates(["id_annonce"])

    show_df_info(df, "Final après nettoyage complet")

    logger.info("Transformation terminée avec succès.")
    return df

# ---------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------
def main():
    """
    Pipeline principal :
    1. Détermine la date du jour (ou utilise la variable d'environnement TARGET_DATE)
    2. Télécharge le fichier Silver du jour depuis S3
    3. Effectue les transformations PySpark
    4. Sauvegarde le résultat au format Parquet dans S3
    """
    # Définition automatique de la date cible
    target_date = os.getenv("TARGET_DATE", datetime.now().strftime("%Y-%m-%d"))

    input_bucket = "paruvendu-data-lake"
    input_key = f"silver/{target_date}.csv"
    output_bucket = "paruvendu-data-lake"
    output_key = f"gold/{target_date}_clean.parquet"

    logger.info(f"Début du pipeline Transform : {input_key} → {output_key}")

    tmp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    tmp_output_dir = tempfile.mkdtemp()

    spark = create_spark_session()

    try:
        # 1. Téléchargement du fichier Silver du jour depuis S3
        download_from_s3(input_bucket, input_key, tmp_input)

        # 2. Lecture du CSV avec Spark
        df_silver = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("encoding", "UTF-8")
            .csv(tmp_input)
        )
        show_df_info(df_silver, f"Lecture initiale du fichier Silver {target_date}")

        # 3. Transformation des données
        df_gold = clean_and_transform(df_silver, tmp_input)

        # 4. Sauvegarde locale du Parquet avant upload
        local_parquet = os.path.join(tmp_output_dir, "data_clean.parquet")
        df_gold.write.mode("overwrite").parquet(local_parquet)
        logger.info(f"Parquet sauvegardé localement : {local_parquet}")

        # 5. Upload du fichier traité vers S3
        upload_to_s3(local_parquet, output_bucket, output_key)

    except Exception as e:
        logger.error(f"Erreur pipeline : {e}")
        raise

    finally:
        # Nettoyage des fichiers temporaires
        try:
            os.remove(tmp_input)
            import shutil
            shutil.rmtree(tmp_output_dir, ignore_errors=True)
            logger.info("Fichiers temporaires supprimés.")
        except Exception as cleanup_err:
            logger.warning(f"Erreur lors du nettoyage : {cleanup_err}")

        # Arrêt propre de Spark
        spark.stop()
        logger.info("Session Spark arrêtée.")
        logger.info(f"Pipeline terminé pour la date {target_date}")

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()

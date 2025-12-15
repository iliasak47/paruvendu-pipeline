import time
import random
import logging
import os
from urllib.parse import urlparse
import requests
from datetime import datetime, timezone
import json
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------
# Configuration du logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def parse_s3_uri(s3_uri: str):
    """
    s3_uri -> ('paruvendu-data-lake', 'raw/')
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("OUTPUT_S3_URI doit commencer par s3://")
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    # parsed.path commence par '/'
    prefix = parsed.path.lstrip("/")
    return bucket, prefix

def put_html_to_s3(s3_client, bucket: str, key: str, html: str, metadata: dict):
    """
    Envoie le HTML sur S3 avec un ContentType explicite et des metadata utiles.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
        Metadata={k: str(v) for k, v in metadata.items()},
    )
    logging.info(f"Upload S3 → s3://{bucket}/{key}")

def scrape_paruvendu(base_url: str, max_pages: int, output_s3_uri: str):
    """
    Télécharge les pages HTML ParuVendu (Raw layer) et les écrit directement sur S3.
    Arborescence S3: s3://<bucket>/<prefix>/YYYY-MM-DD/page_<N>.html
    """
    # S3 setup
    bucket, prefix = parse_s3_uri(output_s3_uri)
    s3 = boto3.client("s3")  # l'AWS CLI déjà configurée suffit

    # Dates 
    scraped_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    partition_date = datetime.now().strftime("%Y-%m-%d")  # Europe/Paris locale

    for page in range(1, max_pages + 1):
        url = f"{base_url}p-{page}/" if page > 1 else base_url
        logging.info(f"Téléchargement : {url}")

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
        except requests.RequestException as e:
            logging.error(f"Erreur de connexion à {url} → {e}")
            return  # stop propre pour Airflow

        if r.status_code >= 400:
            logging.error(f"Erreur HTTP {r.status_code} sur {url}, arrêt du scraping.")
            return  # stop propre

        html = r.text

        # Clé S3 
        key = f"{prefix.rstrip('/')}/{partition_date}/page_{page}.html"

        # Métadonnées
        metadata = {
            "source_url": url,
            "scraped_at_utc": scraped_at_utc,
            "user_agent": HEADERS["User-Agent"][:250],  # limite metadata
            "status_code": r.status_code,
        }

        try:
            put_html_to_s3(s3, bucket, key, html, metadata)
        except (BotoCoreError, ClientError) as e:
            logging.error(f"Erreur S3 sur s3://{bucket}/{key} → {e}")
            return

        # Pause aléatoire
        time.sleep(random.uniform(2, 5))

        # Arrêt si plus d'annonces
        if "Aucune annonce" in html or "0 annonce" in html:
            logging.info("Plus d'annonces, arrêt de la pagination.")
            break


if __name__ == "__main__":
    # ---------------------------
    # Paramètres dynamiques (env)
    # ---------------------------
    base_url = os.getenv(
        "BASE_URL",
        "https://www.paruvendu.fr/immobilier/location/appartement/rouen/?rechpv=1&tt=5&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&px1=600&lo=_76000&ddlFiltres=nofilter&prestige=0",
    )
    max_pages = int(os.getenv("MAX_PAGES", "10"))

    # Utilise une URI S3 
    output_s3_uri = os.getenv("OUTPUT_S3_URI", "s3://paruvendu-data-lake/raw/")

    logging.info(
        json.dumps(
            {
                "base_url": base_url,
                "max_pages": max_pages,
                "output_s3_uri": output_s3_uri,
            },
            ensure_ascii=False,
        )
    )

    scrape_paruvendu(base_url, max_pages, output_s3_uri)

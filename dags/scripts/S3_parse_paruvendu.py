import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import boto3
from urllib.parse import urlparse

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# S3 UTILS
# -------------------------------------------------
def parse_s3_uri(s3_uri: str):
    if not s3_uri.startswith("s3://"):
        raise ValueError("L'URI S3 doit commencer par s3://")
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def list_s3_files(s3_client, bucket: str, prefix: str, suffix=".html"):
    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                files.append(key)
    return files


def read_html_from_s3(s3_client, bucket: str, key: str):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8", errors="ignore")


def write_csv_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8",
    )
    logger.info(f"CSV uploadé → s3://{bucket}/{key}")

# -------------------------------------------------
# CORE PARSING LOGIC
# -------------------------------------------------
def clean_text(txt):
    if not txt:
        return None
    txt = txt.replace("\n", " ").replace("\r", " ").strip()
    txt = txt.replace("m 2", "m²").replace("m2", "m²")
    return " ".join(txt.split())


def parse_paruvendu(input_s3_uri: str, output_s3_uri: str, target_date: str = None):
    s3 = boto3.client("s3")
    input_bucket, input_prefix = parse_s3_uri(input_s3_uri)
    output_bucket, output_prefix = parse_s3_uri(output_s3_uri)

    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    daily_prefix = f"{input_prefix.rstrip('/')}/{target_date}/"
    html_files = list_s3_files(s3, input_bucket, daily_prefix)

    if not html_files:
        logger.warning(f"Aucun fichier HTML trouvé dans {daily_prefix}")
        return

    logger.info(f"{len(html_files)} fichiers HTML détectés pour {target_date}")

    annonces_data = []

    for key in html_files:
        html = read_html_from_s3(s3, input_bucket, key)
        soup = BeautifulSoup(html, "html.parser")

        # Sélecteur robuste : liens immobiliers
        links = soup.select("a[href^='/immobilier/']")
        logger.info(f"{key} → {len(links)} liens d'annonces détectés")

        for a in links:
            try:
                title = a.get("title")
                href = a.get("href")

                if not title or not href:
                    continue

                url = "https://www.paruvendu.fr" + href

                container = a.find_parent("article") or a.parent

                price = None
                description = None
                details = None

                if container:
                    price_tag = container.find(string=lambda t: t and "€" in t)
                    desc_tag = container.find("p")

                    price = clean_text(price_tag) if price_tag else None
                    description = clean_text(desc_tag.get_text(" ", strip=True)) if desc_tag else None
                    details = clean_text(container.get_text(" ", strip=True))

                annonces_data.append({
                    "title": clean_text(title),
                    "details": details,
                    "price": price,
                    "description": description,
                    "url": url,
                    "source_file": key,
                })

            except Exception as e:
                logger.warning(f"Erreur parsing annonce ({key}) : {e}")

    if not annonces_data:
        logger.warning(
            f"Aucune annonce extraite pour {target_date}. "
            "HTML probablement modifié."
        )
        return

    df = pd.DataFrame(annonces_data)

    required_cols = {"title", "url"}
    if not required_cols.issubset(df.columns):
        logger.warning("Colonnes critiques manquantes, parsing ignoré.")
        return

    df.dropna(subset=["title", "url"], inplace=True)
    df = df[df["price"].notna() | df["description"].notna()]

    logger.info(f"Nombre final d'annonces valides : {len(df)}")

    output_key = f"{output_prefix.rstrip('/')}/{target_date}.csv"
    write_csv_to_s3(s3, df, output_bucket, output_key)

    logger.info(f"Parsing terminé → s3://{output_bucket}/{output_key}")

# -------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------
if __name__ == "__main__":
    input_s3_uri = os.getenv("INPUT_S3_URI", "s3://paruvendu-data-lake/raw/")
    output_s3_uri = os.getenv("OUTPUT_S3_URI", "s3://paruvendu-data-lake/silver/")
    target_date = os.getenv("TARGET_DATE")

    logger.info(
        f"Début parsing : {input_s3_uri} → {output_s3_uri} "
        f"(date={target_date or 'today'})"
    )

    parse_paruvendu(input_s3_uri, output_s3_uri, target_date)

    logger.info("Parsing terminé.")

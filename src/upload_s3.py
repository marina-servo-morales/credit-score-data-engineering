from __future__ import annotations

import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def get_s3_client():
    load_dotenv(PROJECT_ROOT / ".env")

    required_variables = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
        "S3_BUCKET_NAME",
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise EnvironmentError(
            f"Variáveis de ambiente ausentes: {missing_variables}"
        )

    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )


def upload_raw_files() -> None:
    s3_client = get_s3_client()
    bucket_name = os.environ["S3_BUCKET_NAME"]

    files_to_upload = [
        RAW_DIRECTORY / "train.csv",
        RAW_DIRECTORY / "test.csv",
    ]

    for local_file in files_to_upload:
        if not local_file.exists():
            raise FileNotFoundError(
                f"Arquivo local não encontrado: {local_file}"
            )

        s3_key = f"credit-score/raw/{local_file.name}"

        logging.info(
            "Enviando %s para s3://%s/%s",
            local_file,
            bucket_name,
            s3_key,
        )

        try:
            s3_client.upload_file(
                Filename=str(local_file),
                Bucket=bucket_name,
                Key=s3_key,
            )
        except (BotoCoreError, ClientError) as error:
            logging.exception(
                "Falha ao enviar %s para o S3.",
                local_file.name,
            )
            raise RuntimeError("Erro no upload para o S3.") from error

    logging.info("Upload de todos os arquivos concluído.")


if __name__ == "__main__":
    upload_raw_files()
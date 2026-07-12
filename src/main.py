import logging

from extract_kaggle import extract_dataset
from upload_s3 import upload_raw_files


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def run_pipeline() -> None:
    logging.info("Pipeline iniciado.")

    extract_dataset()
    upload_raw_files()

    logging.info("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    run_pipeline()
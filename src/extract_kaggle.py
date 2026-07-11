from __future__ import annotations

import logging
import shutil
from pathlib import Path

import kagglehub


DATASET_HANDLE = "parisrohan/credit-score-classification"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def extract_dataset() -> None:
    """Baixa o dataset do Kaggle e copia os arquivos para data/raw."""

    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Iniciando download do dataset: %s",
        DATASET_HANDLE,
    )

    downloaded_directory = Path(
        kagglehub.dataset_download(DATASET_HANDLE)
    )

    logging.info(
        "Dataset baixado no cache: %s",
        downloaded_directory,
    )

    copied_files: list[str] = []

    for source_file in downloaded_directory.rglob("*"):
        if not source_file.is_file():
            continue

        destination_file = RAW_DIRECTORY / source_file.name

        shutil.copy2(
            source_file,
            destination_file,
        )

        copied_files.append(source_file.name)

        logging.info(
            "Arquivo copiado: %s",
            destination_file,
        )

    if not copied_files:
        raise FileNotFoundError(
            "O download foi concluído, mas nenhum arquivo foi encontrado."
        )

    expected_files = {
        "train.csv",
        "test.csv",
    }

    missing_files = expected_files.difference(copied_files)

    if missing_files:
        raise FileNotFoundError(
            f"Arquivos esperados não encontrados: {sorted(missing_files)}"
        )

    logging.info(
        "Extração concluída. Total de arquivos copiados: %d",
        len(copied_files),
    )


if __name__ == "__main__":
    extract_dataset()
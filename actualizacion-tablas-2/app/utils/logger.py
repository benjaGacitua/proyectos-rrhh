import json
import logging
import os
import sys
from datetime import datetime

STAGING_DIR = os.getenv("STAGING_DIR", "/app/staging")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")


class JsonErrorHandler(logging.Handler):
    """
    Writes ERROR+ records to a structured JSON file in the staging directory.
    Format mirrors estado_sincronizacion.json for easy inspection.
    """

    def __init__(self, staging_dir: str):
        super().__init__(level=logging.ERROR)
        os.makedirs(staging_dir, exist_ok=True)
        fecha = datetime.today().strftime("%Y-%m-%d")
        self.filepath = os.path.join(staging_dir, f"errores_etl_{fecha}.json")

    def emit(self, record: logging.LogRecord):
        entry = {
            "fecha": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d"),
            "updated_at": datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["traceback"] = self.formatException(record.exc_info)

        try:
            if os.path.exists(self.filepath):
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = []
        except (json.JSONDecodeError, OSError):
            data = []

        data.append(entry)

        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except OSError:
            pass  # Never crash the ETL because of a log write failure


def setup_logger(name=__name__):
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)

    LOG_FILENAME = f"Registro_inserciones_{datetime.today().date()}.log"
    LOG_PATH = os.path.join(LOG_DIR, LOG_FILENAME)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # File: WARNING+ only → keeps .log files as concise summaries/alerts
        file_handler = logging.FileHandler(LOG_PATH)
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console: INFO+ → full output visible via `docker logs`
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Staging JSON: ERROR+ → structured file in /app/staging/errores_etl_{date}.json
        json_handler = JsonErrorHandler(STAGING_DIR)
        logger.addHandler(json_handler)

    return logger

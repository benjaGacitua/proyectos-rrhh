import logging
import sys
import os
from datetime import datetime

def setup_logger(name=__name__):
    # Crear carpeta logs si no existe
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_FOLDER = os.path.join(BASE_DIR, "logs")
    os.makedirs(LOG_FOLDER, exist_ok=True)
    
    LOG_FILENAME = f"Registro_inserciones_{datetime.today().date()}.log"
    LOG_PATH = os.path.join(LOG_FOLDER, LOG_FILENAME)

    # Configuración básica
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Evitar duplicar handlers si la función se llama varias veces
    if not logger.handlers:
        # Formato
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Handler de Archivo
        file_handler = logging.FileHandler(LOG_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler de Consola (Stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
import pyodbc
import sys
from app.config import settings
from app.utils.logger import setup_logger

logger = setup_logger(__name__)

def get_db_connection():
    try:
        logger.info("Conectando a SQL Server...")
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={settings.DB_SERVER};"
            f"DATABASE={settings.DB_NAME};"
            f"UID={settings.DB_USER};"
            f"PWD={settings.DB_PASSWORD};"
            "ColumnEncryption=Enabled;"      
            "TrustServerCertificate=yes;"
        )
        conexion = pyodbc.connect(conn_str)
        # Habilitar fast_executemany para mejorar velocidad de inserciones masivas
        conexion.autocommit = False 
        return conexion
    except Exception as e:
        logger.error(f"Error crítico al conectar a la base de datos: {e}")
        sys.exit(1)
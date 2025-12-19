import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- Credenciales y Base de Datos ---
TOKEN = os.getenv("TOKEN")
DB_SERVER = os.getenv("SQL_SERVER")
DB_NAME = os.getenv("SQL_DATABASE")
DB_USER = os.getenv("SQL_USER")
DB_PASSWORD = os.getenv("SQL_PASSWORD")


# --- Configuración API ---
API_ENDPOINTS = {
    "licences": f"{os.getenv('API_BASE_URL')}absences/licence",
    "absences": f"{os.getenv('API_BASE_URL')}absences/absence",
    "permissions": f"{os.getenv('API_BASE_URL')}absences/permission"
}

# --- Configuración de Fechas ---
FILTRAR_POR_FECHAS = True # Cambiar a False si quieres traer todo

fecha_hoy = datetime.now().date()
fecha_inicio_objetivo = fecha_hoy - timedelta(days=7)

FECHA_INICIO = fecha_inicio_objetivo.strftime("%Y-%m-%d")
FECHA_FIN = fecha_hoy.strftime("%Y-%m-%d")
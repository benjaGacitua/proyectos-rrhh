import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- Credenciales API ---
TOKEN = os.getenv("TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL")

# --- Credenciales Postgres vía túnel SSH ---
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")
SSH_PRIVATE_KEY_PASSWORD = os.getenv("SSH_PRIVATE_KEY_PASSWORD")

# --- Credenciales Postgres ---
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


# --- Configuración API ---
API_ENDPOINTS = {
    "licences": f"{API_BASE_URL}absences/licence",
    "absences": f"{API_BASE_URL}absences/absence",
    "permissions": f"{API_BASE_URL}absences/permission"
}

# --- Configuración de Fechas ---
FILTRAR_POR_FECHAS = False # Cambiar a False si quieres traer todo

fecha_hoy = datetime.now().date()
fecha_inicio_objetivo = fecha_hoy - timedelta(days=7)

FECHA_INICIO = fecha_inicio_objetivo.strftime("%Y-%m-%d")
FECHA_FIN = fecha_hoy.strftime("%Y-%m-%d")
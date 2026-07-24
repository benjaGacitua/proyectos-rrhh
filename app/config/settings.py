import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- Credenciales API ---
TOKEN = os.getenv("TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL")
URL_SETTLEMENTS_CHILE = os.getenv("URL_SETTLEMENTS_CHILE")
URL_SELECCION = os.getenv("URL_SELECCION")

# --- Credenciales Postgres vía túnel SSH ---
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")
SSH_PRIVATE_KEY_PASSWORD = os.getenv("SSH_PRIVATE_KEY_PASSWORD")

# --- Notificaciones n8n / Telegram ---
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")  # URL del webhook n8n 'airflow-etl-log'

# --- Credenciales Postgres rh_cramer ---
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# --- Credenciales Postgres db_lavanderia ---
LAV_PG_HOST = os.getenv("LAV_PG_HOST", "127.0.0.1")
LAV_PG_PORT = int(os.getenv("LAV_PG_PORT", "5433"))
LAV_PG_DATABASE = os.getenv("LAV_PG_DATABASE")
LAV_PG_USER = os.getenv("LAV_PG_USER", "lavanderia_reader")
LAV_PG_PASSWORD = os.getenv("LAV_PG_PASSWORD")


# --- Configuración API ---
API_ENDPOINTS = {
    "licences": f"{API_BASE_URL}absences/licence",
    "absences": f"{API_BASE_URL}absences/absence",
    "permissions": f"{API_BASE_URL}absences/permission"
}

# --- Configuración de Fechas ---
FILTRAR_POR_FECHAS = True # Cambiar a False si quieres traer todo

fecha_hoy = datetime.now().date()
fecha_inicio_objetivo = fecha_hoy - timedelta(days=14)

FECHA_INICIO = fecha_inicio_objetivo.strftime("%Y-%m-%d")
FECHA_FIN = fecha_hoy.strftime("%Y-%m-%d")
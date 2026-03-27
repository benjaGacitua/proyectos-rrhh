import sys
import atexit
import psycopg2
from sshtunnel import SSHTunnelForwarder
from app.config import settings
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
_ACTIVE_TUNNEL = None

def _build_ssh_tunnel():
    ssh_kwargs = {}

    if settings.SSH_PRIVATE_KEY:
        ssh_kwargs["ssh_pkey"] = settings.SSH_PRIVATE_KEY
        if settings.SSH_PRIVATE_KEY_PASSWORD:
            ssh_kwargs["ssh_private_key_password"] = settings.SSH_PRIVATE_KEY_PASSWORD
    elif settings.SSH_PASSWORD:
        ssh_kwargs["ssh_password"] = settings.SSH_PASSWORD
    else:
        raise ValueError(
            "Debes configurar SSH_PASSWORD o SSH_PRIVATE_KEY para abrir el túnel SSH."
        )

    return SSHTunnelForwarder(
        (settings.SSH_HOST, settings.SSH_PORT),
        ssh_username=settings.SSH_USER,
        remote_bind_address=(settings.PG_HOST, settings.PG_PORT),
        local_bind_address=("127.0.0.1", 0),
        **ssh_kwargs,
    )

def _close_tunnel():
    global _ACTIVE_TUNNEL
    if _ACTIVE_TUNNEL and _ACTIVE_TUNNEL.is_active:
        _ACTIVE_TUNNEL.stop()
    _ACTIVE_TUNNEL = None


atexit.register(_close_tunnel)

def get_db_connection():
    global _ACTIVE_TUNNEL
    try:
        logger.info("Abriendo túnel SSH hacia Postgres...")
        _ACTIVE_TUNNEL = _build_ssh_tunnel()
        _ACTIVE_TUNNEL.start()

        logger.info("Conectando a Postgres por túnel SSH...")
        conexion = psycopg2.connect(
            host="127.0.0.1",
            port=_ACTIVE_TUNNEL.local_bind_port,
            dbname=settings.PG_DATABASE,
            user=settings.PG_USER,
            password=settings.PG_PASSWORD,
            connect_timeout=10,
        )
        conexion.autocommit = False
        return conexion
    except Exception as e:
        _close_tunnel()
        logger.error(f"Error crítico al conectar a la base de datos: {e}")
        sys.exit(1)
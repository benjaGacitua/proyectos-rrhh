# 🚀 (Sincronización API a SQL Server)

Este sistema automatiza la extracción de datos desde la API de la plataforma de Recursos Humanos y la carga en una base de datos SQL Server. 

El sistema soporta **Always Encrypted** de SQL Server mediante el uso de drivers ODBC 18 en un entorno Dockerizado Linux.

---

## 📋 Requisitos del Sistema

* **Docker** y **Docker Compose** instalados en la máquina anfitriona.
* Acceso a la API de origen (Token).
* Certificado `.pfx` para la desencriptación de columnas sensibles.

---

## ⚙️ Configuración del Entorno

Antes de ejecutar el contenedor, es necesario configurar las credenciales y certificados de seguridad.

### 1. Variables de Entorno (.env)
Crea un archivo `.env` en la raíz del proyecto basándote en las variables requeridas en `app/config/settings.py`:

```properties
# Credenciales de API
TOKEN=tu_token_api
API_BASE_URL=[https://api.ejemplo.com/v1/](https://api.ejemplo.com/v1/)

# Base de Datos SQL Server
SQL_SERVER=tuserver.database.windows.net
SQL_DATABASE=IARRHH
SQL_USER=tu_usuario
SQL_PASSWORD=tu_password

# Seguridad (Certificados para Always Encrypted)
PFX_PASSWORD=PassDelCertificadoExportado
CERT_THUMBPRINT=A1B2C3D4E5F67890
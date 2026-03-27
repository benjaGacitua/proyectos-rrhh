FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# --- PASO 1: Instalar dependencias del sistema y GPG ---
# Instalamos primero curl y gnupg para poder descargar las llaves después.
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    build-essential \
    unixodbc-dev \
    openssl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- PASO 2: Instalar Drivers de SQL Server (Método Manual y Seguro) ---
# 1. Descargamos la llave y la guardamos en formato GPG
# 2. Creamos el archivo .list MANUALMENTE con 'echo' para evitar errores de sintaxis o de 'sed'
# 3. Instalamos
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# --- PASO 3: Dependencias de Python ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- PASO 4: Código fuente ---
COPY . .

# --- PASO 5: Comando de ejecución ---
CMD ["python", "app/main.py"]
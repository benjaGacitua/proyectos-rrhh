# 1. Usar una imagen base oficial de Python (ligera)
FROM python:3.12-slim

# 2. Evitar que Python genere archivos .pyc y permitir logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. INSTALACIÓN DE DRIVERS SQL SERVER (ODBC 18) PARA LINUX
# Esto es necesario porque la imagen base es Linux (Debian)
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https build-essential unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean -y

# 5. Copiar los requisitos e instalarlos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copiar el resto del código (la carpeta app)
COPY . .

# 7. Comando para ejecutar la aplicación
# Nota: Docker ejecutará esto al iniciar
CMD ["python", "app/main.py"]
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 1. Agregamos 'openssl' a la lista de instalación
RUN apt-get update && apt-get install -y curl gnupg2 build-essential unixodbc-dev openssl \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 2. Copiamos el script de entrada y le damos permisos de ejecución
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 3. Definimos el script como punto de entrada
ENTRYPOINT ["/entrypoint.sh"]

# 4. El comando por defecto sigue siendo tu app
CMD ["python", "app/main.py"]
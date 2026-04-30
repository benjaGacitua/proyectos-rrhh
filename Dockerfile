FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# --- PASO 1: Dependencias del sistema para psycopg2 y sshtunnel ---
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# --- PASO 2: Dependencias de Python ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- PASO 3: Código fuente ---
COPY . .

# --- PASO 4: Comando de ejecución ---
CMD ["python", "app/main.py"]
#!/bin/bash
set -e

# Definimos rutas
PFX_PATH="/certs/certificado.pfx"
TARGET_DIR="/root/.microsoft/mssql/keys"
TARGET_FILE="$TARGET_DIR/$CERT_THUMBPRINT"

echo "--- Iniciando configuración de certificados ---"

# 1. Crear el directorio donde el driver busca las llaves
mkdir -p "$TARGET_DIR"

# 2. Verificar si existe el archivo .pfx montado
if [ -f "$PFX_PATH" ]; then
    echo "Certificado .pfx encontrado. Convirtiendo..."
    
    # Conversión usando OpenSSL
    # -nodes: No encriptar la llave de salida (necesario para que el driver la lea sin pedir password interactivo)
    # -passin: Pasa la contraseña del PFX mediante variable de entorno
    openssl pkcs12 -in "$PFX_PATH" -out "$TARGET_FILE" -nodes -passin pass:"$PFX_PASSWORD"
    
    chmod 600 "$TARGET_FILE"
    
    echo "Conversión exitosa. Llave guardada en: $TARGET_FILE"
else
    echo "ADVERTENCIA: No se encontró '$PFX_PATH'. Si la base de datos requiere encriptación, esto fallará."
fi

echo "--- Ejecutando aplicación ---"
# Ejecuta el comando CMD del Dockerfile (python app/main.py)
exec "$@"
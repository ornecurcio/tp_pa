# Utiliza la imagen base de Python 3.10 sobre Debian slim
FROM python:3.10-slim-bullseye

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /TP

# Copia el archivo de requerimientos primero para aprovechar la caché de Docker
COPY requirements.txt .

# Instala GCC y cualquier otra dependencia necesaria
RUN apt-get update && \
    apt-get install -y gcc && \
    rm -rf /var/lib/apt/lists/*  # Limpia la cache de APT para reducir el tamaño de la imagen

# Instala las dependencias de Python usando pip
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código fuente de la API al contenedor
COPY apiTp.py .

# Configura el comando por defecto para ejecutar la aplicación
# Corrige el typo de "ubicorn" a "uvicorn"
CMD ["uvicorn", "apiTp:app", "--host", "0.0.0.0", "--port", "8000"]


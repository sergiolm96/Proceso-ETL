FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# Selecionar el directorio de trabajo

WORKDIR /app

# Copiar archivos del proyecto
COPY . .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Punto de entrada
CMD ["python", "-m", "scripts.main_etl"]


# 1. Imagen base
FROM python:3.9-slim

# 2. Definir carpeta de trabajo
WORKDIR /app


# Esto arregla el error "No such file or directory: gcc"
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. Variables de entorno
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 5. Copiar e instalar requerimientos
COPY requirements.txt .
# Actualizamos pip por si acaso
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 6. Descargar modelo de Spacy
RUN python -m spacy download es_core_news_sm

# 7. Copiar código
COPY . .

# 8. Exponer puerto
EXPOSE 5001

# 9. Arrancar (Apuntando a la carpeta interfaz)
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "interfaz.app:app"]

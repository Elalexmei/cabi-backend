# 1. Imagen base de Python ligera
FROM python:3.9-slim

# 2. Definir la carpeta de trabajo
WORKDIR /app

# 3. Variables de entorno para que Python encuentre tus carpetas
# Esto es vital para que 'interfaz/app.py' pueda importar cosas de 'diccionarios' o 'control'
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 4. Copiar e instalar requerimientos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Descargar el modelo de Spacy (Ajusta 'es_core_news_sm' si usas otro)
RUN python -m spacy download es_core_news_sm

# 6. Copiar TODO el código al contenedor
COPY . .

# 7. Exponer el puerto
EXPOSE 5001

# 8. COMANDO DE ARRANQUE (Aquí está el truco para tu estructura)
# Le decimos a Gunicorn: "Busca dentro de la carpeta 'interfaz', el archivo 'app', y la variable 'app'"
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "interfaz.app:app"]
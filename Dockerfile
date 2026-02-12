FROM python:3.13-slim

WORKDIR /app

# Copia os arquivos de requisitos primeiro (otimiza o cache do Docker)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código do projeto
COPY . .

CMD ["python", "main.py"]
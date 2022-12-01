FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -U pip wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY position.json ./
COPY site_init.json ./
COPY test/ ./test/

ENTRYPOINT ["python", "./main.py"]


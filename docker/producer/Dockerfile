FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY producer/ producer/

RUN pip install --no-cache-dir .

CMD ["python", "-m", "telestream_producer.main"]

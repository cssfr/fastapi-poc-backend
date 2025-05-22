FROM python:3.11-slim

WORKDIR /app

# install curl for Coolify’s healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 80
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

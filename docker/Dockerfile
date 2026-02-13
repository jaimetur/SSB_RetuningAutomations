FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Default value if PORT is not provided at runtime (via docker compose env_file / environment).
ENV PORT=7878

# IMPORTANT: use "sh -c" so ${PORT} is expanded at container runtime (JSON-form CMD does not expand variables).
CMD ["sh", "-c", "exec uvicorn src.web_interface.web_interface:app --host 0.0.0.0 --port ${PORT}"]
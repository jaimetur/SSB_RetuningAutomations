FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 7878

# Run the FastAPI app from the refactored src/webapp package.
CMD ["uvicorn", "src.webapp.webapp:app", "--host", "0.0.0.0", "--port", "7878"]

FROM python:3.12-slim

# Prevents Python from writing .pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Ensures stdout/stderr are unbuffered so logs stream immediately to
# Vertex AI / Cloud Logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# # 4. Install System Dependencies (Optional but recommended)
# # Include this if any of your Python dependencies require compiling C extensions
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

COPY . /app/

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "data_science_adk"]

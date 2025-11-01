# Use a lightweight official Python base image
FROM python:3.13


# Create and set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .
COPY ./proxies.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY ./search.py .

# Run the script
CMD ["python3", "search.py"]

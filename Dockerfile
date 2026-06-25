# We are using a fast version of Python.
FROM python:3.11-slim

# Our working folder inside the container
WORKDIR /app

# First, copy and install the library list.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then copy all our code (Producer, Consumer, Server, etc.) inside.
COPY . .
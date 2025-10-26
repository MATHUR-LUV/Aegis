# Use a slim Python image
FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main application code
COPY main.py .

# Expose the port FastAPI will run on
EXPOSE 8000

# Command to run the Uvicorn server
# --host 0.0.0.0 makes it accessible to other Docker containers
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

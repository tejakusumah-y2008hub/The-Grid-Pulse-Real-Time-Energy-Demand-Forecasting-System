# Start with the official Airflow image
FROM apache/airflow:2.10.4-python3.10

# Switch to the root user to install system dependencies (if needed)
USER root

# Switch back to the airflow user to install Python packages
USER airflow

# Copy the requirements file into the container
COPY requirements.txt .

# Install the libraries
RUN pip install --no-cache-dir -r requirements.txt
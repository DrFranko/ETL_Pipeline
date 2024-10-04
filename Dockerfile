# Use OpenJDK 8 on a Debian-based image as base image for better compatibility
FROM openjdk:8-jdk-slim

# Install essential tools and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    bash curl gfortran build-essential liblapack-dev python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.4.0-bin-hadoop3.tgz && \
    mv spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy requirements file into the container
COPY requirements.txt .

# Install Python dependencies (including pandas and numpy)
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application code
COPY . .

# Specify default command to run your PySpark model
CMD ["python3", "main.py"]
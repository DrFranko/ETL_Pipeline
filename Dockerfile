# Base image
FROM openjdk:8-jdk-alpine

# Install PySpark dependencies
RUN apk add --no-cache bash curl && \
    curl -O https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz && \
    mv spark-3.4.0-bin-hadoop3 /opt/spark

# Set environment variables for Spark
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Install Python and dependencies
RUN apk add --no-cache python3 py3-pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy the application code
WORKDIR /app
COPY . .

# Set default command
CMD ["spark-submit", "--master", "local[*]", "your_script.py"]
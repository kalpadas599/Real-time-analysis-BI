# Use Python 3.9 as the base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*


# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK resources (including vader_lexicon)
RUN python -c "import nltk; nltk.download('vader_lexicon')"

# Download SpaCy model
RUN python -m spacy download en_core_web_sm

# Copy application code
COPY . .


# Expose port for API
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]
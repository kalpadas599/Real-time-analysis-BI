# Use Python 3.9 as the base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-22-jdk \
    && rm -rf /var/lib/apt/lists/*


# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set NLTK data directory
ENV NLTK_DATA=/usr/local/share/nltk_data

# Download NLTK resources (including vader_lexicon)
RUN python -c "import nltk; nltk.download('vader_lexicon')"

# Copy the SpaCy model tarball
#COPY en_core_web_sm-3.8.0.tar.gz /tmp/

# Install the SpaCy model
#RUN pip install /tmp/en_core_web_sm-3.8.0.tar.gz && \
#    rm /tmp/en_core_web_sm-3.8.0.tar.gz

# Download and install the SpaCy model
RUN python -m spacy download en_core_web_sm

# Copy application code
COPY . .


# Expose port for API
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]
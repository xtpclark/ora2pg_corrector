FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Ora2Pg and PostgreSQL
RUN apt-get update && apt-get install -y \
    perl \
    libdbi-perl \
    libpq-dev \
    build-essential \
    wget \
    unzip \
    make \
    cpanminus \
    && rm -rf /var/lib/apt/lists/*

# Install Ora2Pg
RUN wget https://github.com/darold/ora2pg/archive/refs/tags/v24.3.tar.gz \
    && tar -xzf v24.3.tar.gz \
    && cd ora2pg-24.3 \
    && perl Makefile.PL \
    && make && make install \
    && cd .. && rm -rf ora2pg-24.3 v24.3.tar.gz

# Install Perl dependencies for Ora2Pg (only DBD::Pg for PostgreSQL)
RUN cpanm DBD::Pg

# Copy application files
COPY requirements.txt .
COPY server.py .
COPY api_connector.py .
COPY ora2pg_ai_corrector.py .
COPY static/ static/
COPY .env .env

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "server:app"]

# Use an official Python runtime as a parent image
FROM python:3.9-slim-bullseye

# Arguments for user and group IDs
ARG HOST_UID=1000
ARG HOST_GID=1000

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    perl \
    libdbi-perl \
    libpq-dev \
    build-essential \
    unzip \
    make \
    cpanminus \
    gosu \
    libaio1 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy local Oracle Instant Client files and install them
WORKDIR /opt/oracle
COPY oracle_instantclient/*.zip .
RUN \
  unzip -o instantclient-basic-linux.x64-21.19.0.0.0dbru.zip && \
  unzip -o instantclient-sdk-linux.x64-21.19.0.0.0dbru.zip && \
  unzip -o instantclient-sqlplus-linux.x64-21.19.0.0.0dbru.zip && \
  rm -f *.zip && \
  sh -c 'mv instantclient_* instantclient'

# Configure environment for Oracle Client
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$ORACLE_HOME
# --- CHANGE: Add Oracle client to the system's executable PATH ---
ENV PATH=$ORACLE_HOME:$PATH
# Update dynamic linker cache
RUN ldconfig

# Set working directory back to /app
WORKDIR /app

# Install Ora2Pg from source
RUN wget https://github.com/darold/ora2pg/archive/refs/tags/v24.3.tar.gz \
    && tar -xzf v24.3.tar.gz \
    && cd ora2pg-24.3 \
    && perl Makefile.PL \
    && make && make install \
    && cd .. && rm -rf ora2pg-24.3 v24.3.tar.gz

# Install Perl dependencies for Ora2Pg (PostgreSQL and Oracle)
RUN cpanm DBD::Pg
RUN ORACLE_HOME=${ORACLE_HOME} LD_LIBRARY_PATH=${LD_LIBRARY_PATH} cpanm --force DBD::Oracle

# Create a non-root user that will run the application
RUN groupadd -g ${HOST_GID} appuser || groupadd appuser
RUN useradd -u ${HOST_UID} -g appuser -m -s /bin/bash appuser

# Copy application files (chown will be handled by entrypoint)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Copy and set up the entrypoint script
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]

# Expose port
EXPOSE 8000

# Command to run the application (will be executed by the entrypoint)
# CMD ["gunicorn", "--timeout", "3000", "-w", "4", "-b", "0.0.0.0:8000", "app:create_app()"]
CMD ["gunicorn", "--log-level", "debug", "--error-logfile", "-", "--timeout", "3000", "-w", "4", "-b", "0.0.0.0:8000", "app:create_app()"]

FROM quay.io/astronomer/astro-runtime:11.3.0

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Create the keyrings directory
RUN mkdir -p /etc/apt/keyrings

# Add Docker’s official GPG key
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the Docker repository for Debian
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list

# Update the package database again to include Docker packages
RUN apt-get update

# Install Docker CLI
RUN apt-get install -y docker-ce-cli

USER astro

WORKDIR /app

COPY requirements_ex.txt /app/requirements_ex.txt

RUN pip install --no-cache-dir -r /app/requirements_ex.txt

COPY dags/module/example/example_train.py /app/example_train.py

CMD ["python", "/app/example_train.py"]

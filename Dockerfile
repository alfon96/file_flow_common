FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git tree \
 && rm -rf /var/lib/apt/lists/*

# Create vscode user (UID/GID 1000 for VS Code devcontainers)
RUN groupadd --gid 1000 vscode && \
    useradd --uid 1000 --gid 1000 -m vscode

WORKDIR /app

# Install Python dependencies first (better cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# Switch to non-root user for VS Code
USER vscode

# Keep container alive while attached in devcontainer
CMD ["sleep", "infinity"]

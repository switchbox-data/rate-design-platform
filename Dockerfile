FROM python:3.11-slim

WORKDIR /app

# Copy project metadata first to leverage Docker layer caching.
COPY pyproject.toml uv.lock ./

# Install project (no dependencies listed yet; add as you build).
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Copy the rest of the source tree.
COPY . .

CMD ["bash"]

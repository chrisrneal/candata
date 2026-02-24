FROM python:3.12-slim

WORKDIR /app

# Install shared package first
COPY shared/python /tmp/candata-shared
RUN pip install --no-cache-dir /tmp/candata-shared && rm -rf /tmp/candata-shared

# Install API package
COPY packages/api /tmp/candata-api
RUN pip install --no-cache-dir /tmp/candata-api && rm -rf /tmp/candata-api

EXPOSE 8000

CMD ["uvicorn", "candata_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

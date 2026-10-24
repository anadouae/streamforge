FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schemas schemas
COPY streamforge streamforge
COPY producers producers
COPY consumers consumers
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "producers/traffic_producer.py"]
CMD ["--rounds", "2"]

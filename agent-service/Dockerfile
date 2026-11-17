FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml main.py ./
COPY core ./core
COPY tools ./tools
COPY models ./models
COPY memory ./memory
COPY agents ./agents
COPY orchestration ./orchestration
COPY decision ./decision
COPY api ./api
COPY rag ./rag
COPY observability ./observability
COPY ops ./ops
COPY evaluations ./evaluations
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" . && useradd --create-home appuser && mkdir -p /app/data && chown appuser:appuser /app/data
USER appuser
EXPOSE 8090
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]

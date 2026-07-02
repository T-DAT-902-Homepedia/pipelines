# Image unique du pipeline : chaque étape du DAG (Cloud Workflows) exécute ce
# conteneur avec des arguments CLI différents (cf. ADR-0009).
FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .[gcs]

# Pré-installe l'extension spatial au build : évite un téléchargement réseau
# d'extension à chaque exécution du job.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL spatial')"

ENTRYPOINT ["python", "-m", "duckpipe"]

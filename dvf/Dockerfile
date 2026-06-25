# Image batch du pipeline Kedro DVF.
# Reproduit l'env local validé : Python 3.13 + Java 17 (Spark 3.5 exige Java 17,
# 21 non supporté). PySpark 3.5.8 (pip) embarque déjà Spark + Hadoop 3.3.4, donc
# on n'installe AUCUN Spark système (contrairement à kedro-docker --with-spark).
#
# Base Bookworm (Debian 12) explicite : Trixie (13) a retiré openjdk-17 de ses
# dépôts (passé à 21+), or Spark 3.5 ne supporte pas Java 21.
FROM python:3.13-slim-bookworm

# JDK 17 (headless). procps fournit `ps`, attendu par certains scripts/outils
# Spark (introspection de processus) — peu coûteux, gardé par prudence.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
 && rm -rf /var/lib/apt/lists/*

# JAVA_HOME résolu dynamiquement depuis le binaire java réel (robuste amd64/arm64).
RUN JAVA_BIN="$(readlink -f "$(command -v java)")" \
 && JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")" \
 && echo "JAVA_HOME=$JAVA_HOME" > /etc/profile.d/java_home.sh \
 && ln -sfn "$JAVA_HOME" /usr/lib/jvm/current
ENV JAVA_HOME=/usr/lib/jvm/current
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Utilisateur non-root (defense-in-depth). UID 1000 = UID hôte par défaut, donc
# droits cohérents sur le volume bind data/01_raw (téléchargement du CSV).
# Home = /app : la JVM lit user.home depuis /etc/passwd (pas $HOME) pour situer
# le cache Ivy (~/.ivy2) -> il faut que le home système soit /app, sinon le
# volume ivy_cache monté sur /app/.ivy2 ne capterait rien.
RUN useradd --no-create-home --home-dir /app --uid 1000 kedro

WORKDIR /app

# Dépendances Python d'abord (cache de build).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Source + config du projet, puis installation du package `dvf` (entry points kedro).
COPY pyproject.toml README.md ./
COPY src ./src
COPY conf ./conf
# Crée .ivy2 possédé par kedro : un volume nommé vide hérite des perms de ce
# point de montage à l'init, sinon il serait root et non inscriptible.
RUN pip install --no-cache-dir -e . \
 && mkdir -p /app/.ivy2 \
 && chown -R kedro:kedro /app

# HOME=/app : Ivy écrit dans /app/.ivy2, persisté via un volume nommé (compose).
ENV HOME=/app
USER kedro

# Le CSV brut n'est PAS copié (gitignoré, ~94 Mo) : téléchargé au runtime par le
# node ensure_raw_csv, ou monté depuis l'hôte (volume compose).
ENTRYPOINT ["kedro", "run"]

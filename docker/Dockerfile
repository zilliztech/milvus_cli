FROM python:3.11-slim

ARG VERSION
RUN pip install --no-cache-dir milvus-cli==${VERSION}

ENTRYPOINT ["milvus_cli"]

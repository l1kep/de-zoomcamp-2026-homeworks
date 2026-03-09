FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git tini \
    python3 python3-pip python3-venv \
    openjdk-21-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /workspace

# Python deps (pyspark + jupyter)
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN uv pip install --python /opt/venv/bin/python \
    jupyterlab notebook ipykernel \
    pyspark

# Spark UI / driver binding tweaks for Docker
ENV SPARK_LOCAL_IP=0.0.0.0
ENV SPARK_DRIVER_BIND_ADDRESS=0.0.0.0

EXPOSE 8888 4040 4041 4042 4043 4044 4045 4046 4047 4048 4049 4050

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.token=", "--ServerApp.password=", "--allow-root"]
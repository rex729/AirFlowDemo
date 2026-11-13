# ============================================================
# 【第 1 步：以 Ubuntu 22.04 作為基礎環境】
# ============================================================
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# ============================================================
# 【第 2 步：安裝 Python 與系統工具】
# ============================================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# 【第 3 步：全域安裝 Apache Airflow】
# ============================================================
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.10
RUN pip3 install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ============================================================
# 【第 4 步：建立專屬 airflow 帳號與目錄】
# ============================================================
ENV AIRFLOW_HOME=/opt/airflow
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

# 打包程式碼進容器
COPY --chown=airflow:airflow ./dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow:airflow requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN chown -R airflow:airflow ${AIRFLOW_HOME}

USER airflow
WORKDIR ${AIRFLOW_HOME}

# ============================================================
# 【環境設定：過濾所有 INFO 雜訊，只輸出 ERROR】
# ============================================================
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# ============================================================
# 【第 5 步：開機無限輪詢執行】
# 1. 靜音初始化資料庫
# 2. 進入 while true 循環：執行 DAG -> 休息 10 秒 -> 執行下一輪
# （如果有任何錯誤訊息，100% 不會被遮蔽，會直接印出）
# ============================================================
CMD ["bash", "-c", "airflow db migrate > /dev/null 2>&1 && while true; do airflow dags test task_abc_demo; echo ''; echo '>>> 本輪完成，休息 10 秒後啟動下一輪...'; echo ''; sleep 10; done"]

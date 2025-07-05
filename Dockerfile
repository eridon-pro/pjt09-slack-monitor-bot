FROM python:3.12-slim
WORKDIR /usr/src/app

# Packeage Installation
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
      tzdata sqlite3 openssh-client \
      build-essential python3-dev libatlas-base-dev gfortran && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Tokyo

# Python Module Installation
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy of Applicatioin Source code
COPY . .

CMD ["python", "app.py"]
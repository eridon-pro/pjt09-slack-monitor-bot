FROM python:3.12-slim
WORKDIR /usr/src/app

# システム依存パッケージ（sqlite3 や dateutil などが必要なら）
RUN apt-get update && apt-get install -y tzdata sqlite3 openssh-client && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Tokyo

# Python 依存関係
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# アプリのソースをコピー
COPY . .

CMD ["python", "app.py"]
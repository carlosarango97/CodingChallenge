FROM python:latest

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN apk update
RUN apk add gcc unixodbc-dev
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "py_scripts/API_listener.py"]
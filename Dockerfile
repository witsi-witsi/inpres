FROM python:3.8-buster

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN pip install git+https://github.com/witsi-witsi/witsi.git

WORKDIR /app

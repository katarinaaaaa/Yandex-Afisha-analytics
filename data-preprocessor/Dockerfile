FROM python:3

WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt
RUN mkdir -p /app/storage

CMD ["python3", "-u", "prepare_data.py"]
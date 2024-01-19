FROM python:3

WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt
RUN mkdir -p /app/storage

ENV PERIOD day

CMD ["sh", "-c", "python3 -u get_all_data.py --pool ${PERIOD}"]
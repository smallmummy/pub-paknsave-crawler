FROM python:3.7.4

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    ln -sf /usr/share/zoneinfo/Pacific/Auckland /etc/localtime && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /vc_code
COPY . /vc_code/

CMD python3 run.py
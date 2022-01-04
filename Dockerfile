FROM python:3.8

WORKDIR /data/dflow
ADD requirements.txt ./
RUN pip install -r requirements.txt
COPY ./ ./
RUN pip install . -i https://pypi.tuna.tsinghua.edu.cn/simple


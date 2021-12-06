FROM python:3.8

WORKDIR /data/concurrent-learning-framework
COPY ./ ./
RUN pip install . -i https://pypi.tuna.tsinghua.edu.cn/simple

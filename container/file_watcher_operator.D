FROM python:3.10

RUN pip install kopf
RUN pip install kubernetes

RUN mkdir /src
ADD file_watcher_operator/file_watcher_operator.py /src

CMD kopf run /src/file_watcher_operator.py --verbose
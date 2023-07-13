FROM python:3.10

RUN mkdir /src
ADD file_watcher_operator/file_watcher_operator.py /src

RUN pip install kopf
RUN pip install kubernetes

CMD kopf run /src/file_watcher_operator.py --verbose
FROM python:3.10

ADD . .
RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["file_watcher"]
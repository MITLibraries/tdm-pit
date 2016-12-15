FROM python:3.5
MAINTAINER Mike Graves <mgraves@mit.edu>

COPY pit /pit/pit
COPY requirements.* /pit/
COPY LICENSE /pit/
COPY setup.* /pit/

RUN python3.5 -m pip install -r /pit/requirements.txt
RUN python3.5 -m pip install /pit/

ENTRYPOINT ["pit"]
CMD ["--help"]

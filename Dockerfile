FROM ubuntu:20.04

RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
RUN apt install -y python3 python3-pip libsecp256k1-dev libleveldb-dev pkg-config build-essential && apt clean

RUN mkdir chainalytic_icon
WORKDIR /chainalytic_icon
COPY src src
COPY launch.py launch.py
COPY README.adoc README.adoc
COPY setup.py setup.py

RUN /usr/bin/python3 -m pip install -e .

ENTRYPOINT /usr/bin/python3 launch.py --keep-running

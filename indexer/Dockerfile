FROM ubuntu:20.04

RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt-get update && apt-get install -y git cmake wget python3 python3-pip && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN python3 -m pip install -U pip

# python requirements
ADD requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

# app
COPY . /app
WORKDIR /app

# entrypoint
ENV C_FORCE_ROOT 1
ENTRYPOINT [ "/app/entrypoint.sh" ]

FROM python:3.12-bookworm

# python requirements
ADD requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

# app
COPY . /app
WORKDIR /app

# entrypoint
ENV C_FORCE_ROOT 1
ENTRYPOINT [ "/app/entrypoint.sh" ]

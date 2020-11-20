FROM python:3.7-alpine

RUN apk update && \
    apk add --no-cache gcc musl-dev linux-headers libffi-dev libressl-dev make g++

# Create a group and user
RUN addgroup -S funcx && adduser -S funcx -G funcx
WORKDIR /opt/funcx-forwarder
COPY requirements.txt .
RUN git clone https://github.com/funcx-faas/funcX.git
RUN pip install funcx/funcx_endpoint
RUN pip install -r requirements.txt

COPY . /opt/funcx-forwarder
RUN pip install .

USER funcx
WORKDIR /home/funcx
EXPOSE 55000-56000
EXPOSE 3031
ENTRYPOINT sh /opt/funcx-forwarder/entrypoint.sh


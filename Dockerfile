FROM python:3.7-alpine

RUN apk update && \
    apk add --no-cache gcc musl-dev linux-headers libffi-dev libressl-dev make g++ git

# Create a group and user
RUN addgroup -S funcx && adduser -S funcx -G funcx
WORKDIR /opt/funcx-forwarder

RUN pip install "git+https://github.com/funcx-faas/funcX.git@forwarder_rearch_1#egg=funcx&subdirectory=funcx_sdk"
RUN pip install "git+https://github.com/funcx-faas/funcX.git@forwarder_rearch_1#egg=funcx_endpoint&subdirectory=funcx_endpoint"
RUN python3 -c "import funcx_endpoint; print(funcx_endpoint.__version__)"

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /opt/funcx-forwarder
RUN pip install .

USER funcx
WORKDIR /home/funcx
EXPOSE 55000-56000
EXPOSE 3031
ENTRYPOINT sh /opt/funcx-forwarder/entrypoint.sh


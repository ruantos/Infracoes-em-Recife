FROM ubuntu:latest
LABEL authors="ruantos"

ENTRYPOINT ["top", "-b"]
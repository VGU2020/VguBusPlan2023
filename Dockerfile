FROM python:3.12-slim

ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /requirements.txt

# Install individual dependencies
# so that we could avoid installing extra packages to the container
RUN pip3 --default-timeout=100 install -r requirements.txt

# Create work dicrectory
RUN mkdir /app
WORKDIR /app
COPY ./app /app

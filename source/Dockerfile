FROM centos/python-38-centos7
MAINTAINER Sreenivasulu Bachu <sreenivasulu.bachu@seagate.com>
USER root
RUN pip install pika

RUN mkdir /apps/rtm/ -p
WORKDIR /apps/rtm/
RUN mkdir /apps/rtm/rabbitmqtest -p
WORKDIR /apps/rtm/rabbitmqtest/

COPY . .

# CMD [ "python", "./clientPublish.py" ]
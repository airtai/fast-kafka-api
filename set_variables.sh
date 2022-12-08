#!/bin/bash
if test -z "$AIRT_PROJECT"; then
      echo 'AIRT_PROJECT variable not set, setting to current directory'
      export AIRT_PROJECT=`pwd`
fi
echo AIRT_PROJECT variable set to $AIRT_PROJECT

export UID=$(id -u)
export GID=$(id -g)

export DOCKER_COMPOSE_PROJECT="${USER}-fast-kafka-api"
echo DOCKER_COMPOSE_PROJECT variable set to $DOCKER_COMPOSE_PROJECT
export KAFKA_HOSTNAME="${DOCKER_COMPOSE_PROJECT}-kafka-1"
echo KAFKA_HOSTNAME variable set to $KAFKA_HOSTNAME
export PRESERVE_ENVS="KAFKA_HOSTNAME,KAFKA_PORT"
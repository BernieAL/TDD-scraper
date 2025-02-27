#!/bin/bash

# Variables
IMAGE_NAME="my_postgres_image"
CONTAINER_NAME="my_postgres_container"
POSTGRES_PORT=5432
POSTGRES_DB="designer_products"
POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin!"
DOCKERFILE="Dockerfile.pgsql"

# Check if a container with the same name is already running
if [ $(docker ps -aq -f name=$CONTAINER_NAME) ]; then
    echo "Stopping and removing existing container: $CONTAINER_NAME"
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Build the Docker image using Dockerfile.pgsql
echo "Building Docker image: $IMAGE_NAME using $DOCKERFILE"
docker build -f $DOCKERFILE -t $IMAGE_NAME .

# Run the Docker container
echo "Running Docker container: $CONTAINER_NAME"
docker run -d \
    --name $CONTAINER_NAME \
    -e POSTGRES_DB=$POSTGRES_DB \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -p $POSTGRES_PORT:5432 \
    $IMAGE_NAME

echo "Container $CONTAINER_NAME is up and running."

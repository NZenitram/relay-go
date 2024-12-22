#!/bin/bash

# Variables
REGION="us-east-2"
ACCOUNT_ID="257394459269"
REPOSITORY_NAME="sh-relay-go/sh-relay-go-ingest"
IMAGE_NAME="relay-go"
TAG="build"

# Build the Docker image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} .

# Authenticate Docker to the ECR registry
echo "Logging into ECR..."
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Tag the image for ECR
echo "Tagging the image..."
docker tag ${IMAGE_NAME}:${TAG} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}:${TAG}

# Push the image to ECR
echo "Pushing the image to ECR..."
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}:${TAG}

echo "Done! Image pushed to ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}:${TAG}"

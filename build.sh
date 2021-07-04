readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/chord-node
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .

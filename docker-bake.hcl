variable "REGISTRY" {
  default = "registry.diginfra.net/tsd"
}

variable "IMAGE_NAME" {
  default = "hi-bataafse-politieke-tijdschriften-indexer"
}

variable "MONGO_IMAGE_NAME" {
  default = "hi-bataafse-politieke-tijdschriften-mongo"
}

variable "TAG" {
  default = "latest"
}

function "image_ref" {
  params = [name]
  result = REGISTRY != "" ? "${REGISTRY}/${name}:${TAG}" : "${name}:${TAG}"
}

group "default" {
  targets = ["indexer", "mongo"]
}

target "indexer" {
  context    = "."
  dockerfile = "Dockerfile.indexer"
  tags       = [image_ref(IMAGE_NAME)]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "mongo" {
  context    = "."
  dockerfile = "Dockerfile.mongo"
  tags       = [image_ref(MONGO_IMAGE_NAME)]
  platforms  = ["linux/amd64", "linux/arm64"]
}

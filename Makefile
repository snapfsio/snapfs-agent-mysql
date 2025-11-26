# Makefile

# --- Configurable bits per repo -----------------------------------
REGISTRY ?= ghcr.io
ORG      ?= snapfsio
IMAGE    ?= snapfs-agent-mysql
TAG      ?= latest

# Optional: also tag with short git SHA
GIT_SHA  := $(shell git rev-parse --short HEAD)

IMAGE_LOCAL  := $(IMAGE):$(TAG)
IMAGE_REMOTE := $(REGISTRY)/$(ORG)/$(IMAGE):$(TAG)
IMAGE_SHA    := $(REGISTRY)/$(ORG)/$(IMAGE):$(GIT_SHA)

.PHONY: build tag push publish login

# Build local image
build:
	sudo docker build -t $(IMAGE_LOCAL) .

# Tag for ghcr
tag: build
	sudo docker tag $(IMAGE_LOCAL) $(IMAGE_REMOTE)
	sudo docker tag $(IMAGE_LOCAL) $(IMAGE_SHA)

# Push tags to ghcr
push: tag
	sudo docker push $(IMAGE_REMOTE)
	sudo docker push $(IMAGE_SHA)

# Convenience: build + tag + push
publish: push

# One-time (per machine) login to ghcr
login:
	echo "$$GHCR_TOKEN" | sudo docker login ghcr.io -u $(ORG) --password-stdin

IMAGE = ttl.sh/egd-processor

build-and-push:
	make build
	make push

build:
	docker build --platform linux/amd64 -t $(IMAGE) .

push:
	docker push $(IMAGE)

TAG = $(shell git describe --tag)
DOCKER_RUN = docker-compose run
DOCKER_DOWN = docker-compose down

.env:
	cp -n .env.template .env > /dev/null || :

_compose_build:
	docker-compose build sidecar
.PHONY: _compose_build

run: .env _compose_build
	$(DOCKER_DOWN)
	$(DOCKER_RUN) -p 8989:8989 sidecar
	$(DOCKER_DOWN)
.PHONY: run

build:
	docker build -t turbosonic/event-hub-sidecar .
.PHONY: build

tag:
	docker tag turbosonic/event-hub-sidecar turbosonic/event-hub-sidecar:$(TAG)
.PHONY: tag

login:
	docker login -u $(DOCKER_USERNAME) -p $(DOCKER_PASSWORD)
.PHONY: login

push:
	docker push turbosonic/event-hub-sidecar:$(TAG)
.PHONY: push
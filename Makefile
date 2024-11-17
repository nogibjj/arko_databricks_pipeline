install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	pytest tests/test_*.py -v


format:	
	black modules/*.py tests/*.py

lint:
	ruff check modules/*.py tests/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < .devcontainer/Dockerfile


deploy:

	docker build -f .devcontainer/Dockerfile -t elt_pipeline:latest .

	docker rm -f elt_pipeline

	docker run -d --name elt_pipeline -p 80:80 elt_pipeline:latest

	echo "Deployment completed."
		
all: install format lint test

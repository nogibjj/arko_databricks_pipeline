install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:	
	black *.py 

lint:
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < .devcontainer/Dockerfile

refactor: format lint

deploy:

	docker build -f .devcontainer/Dockerfile -t arko_cli_tool:latest .

	docker rm -f arko_cli_tool

	docker run -d --name arko_cli_tool -p 80:80 arko_cli_tool:latest

	echo "Deployment completed."
		
all: install lint test format deploy

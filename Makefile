venv:
	rm -rf venv && \
	python3 -m venv venv && \
	. venv/bin/activate && \
	python3 -m pip install -r requirements.dev.txt &&\
	pyenv local 3.7.7 3.8.2
pytest:
	. venv/bin/activate; \
	python3 -m pytest
pylint:
	. venv/bin/activate; \
	python3 -m pylint src
test: requirements.txt requirements.dev.txt
	. venv/bin/activate; \
	tox
upload:
	. venv/bin/activate; \
	rm -rf dist; \
	python3 setup.py sdist bdist_wheel; \
	python3 -m twine check dist/*; \
	python3 -m twine upload dist/*
clean:
	rm -rf venv
	rm -rf dist
	rm -rf .tox
	find . -name '__pycache__' -delete

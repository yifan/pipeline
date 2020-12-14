venv: requirements.txt requirements.dev.txt
	rm -rf venv && \
	python3 -m venv venv && \
	. venv/bin/activate && \
	python3 -m pip install -r requirements.dev.txt -r requirements.txt
pytest:
	. venv/bin/activate; \
	python3 -m pytest
pylint:
	. venv/bin/activate; \
	python3 -m pylint src
test:venv
	. venv/bin/activate; \
	tox
upload:
	. venv/bin/activate; \
	rm -rf dist; \
	python3 setup.py sdist bdist_wheel; \
	python3 -m twine check dist/*; \
	python3 -m twine upload dist/*
integration:
	. venv/bin/activate; \
	rm -rf integration-tests/dist; \
	python3 setup.py bdist_wheel -d integration-tests/dist;
clean:
	rm -rf venv
	rm -rf dist
	rm -rf .tox
	find . -name '__pycache__' -delete

venv:
	rm -rf venv
	python3 -m venv --system-site-packages venv
	. venv/bin/activate
	python3 -m pip install -r requirements.txt
	python3 -m pip install -r requirements.dev.txt
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

.PHONY: docs
init:
	rm -rf venv
	python3 -m venv --system-site-packages venv
	. venv/bin/activate
	python3 -m pip install -r requirements.txt
	python3 -m pip install -r requirements.dev.txt
pytest:
	. venv/bin/activate; \
	python3 -m pytest
test:
	. venv/bin/activate; \
	tox
upload:
	. venv/bin/activate; \
	rm -rf dist; \
	python3 setup.py sdist bdist_wheel; \
	python3 -m twine check dist/*; \
	python3 -m twine upload --config-file ~/.pypirc --verbose -r testpypi dist/*; \
	python3 -m twine upload dist/*
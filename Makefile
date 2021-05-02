VENV=venv
PY=python3
VPY=$(VENV)/bin/$(PY)
REQUIREMENTS = requirements.txt requirements.full.txt requirements.dev.txt
PIPSTR = $(foreach wrd, $(REQUIREMENTS), -r $(wrd))

.PHONY: venv
$(VPY): $(VENV)
	$(PY) -m venv $(VENV) && \
	$(VPY) -m pip install $(PIPSTR)

.PHONY: pytest
pytest: $(VPY)
	$(VPY) -m pytest $(ARGS)

.PHONY: coverage
coverage: $(VPY)
	PYTHONPATH=src $(VPY) -m pytest --cov=src --cov-report term-missing tests src $(ARGS)

.PHONY: pylint
pylint: $(VPY)
	$(VPY) -m pylint $(ARGS) src

.PHONY: mypy
mypy: $(VPY)
	$(VPY) -m mypy --ignore-missing-imports src

.PHONY: test
test: $(VPY)
	$(VPY) -m tox $(ARGS)

.PHONY: upload
upload: $(VPY) test
	rm -rf dist; \
	$(VPY) setup.py sdist bdist_wheel; \
	$(VPY) -m twine check dist/*; \
	$(VPY) -m twine upload dist/*

integration: $(VPY)
	rm -rf integration-tests/dist; \
	$(VPY) setup.py bdist_wheel -d integration-tests/dist;

.PHONY: clean
clean:
	rm -rf venv
	rm -rf dist
	rm -rf .tox
	find . -name '__pycache__' -delete

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
	$(VPY) -m pytest

.PHONY: pylint
pylint: $(VPY)
	$(VPY) -m pylint src

.PHONY: mypy
mypy: $(VPY)
	$(VPY) --ignore-missing-imports src

.PHONY: test
test: $(VPY)
	$(VPY) -m tox

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

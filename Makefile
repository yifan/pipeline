.PHONY: docs
init:
	python3 -m venv venv
	python3 -m pip install -r requirements.txt
	python3 -m pip install tox
test:
	. venv/bin/activate
	tox

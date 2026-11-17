test:
	python3 -m unittest discover -s tests -v

build:
	python3 -m compileall -q privacywarehouse tests

demo:
	python3 -m privacywarehouse

VENV_DIR = venv
PYTHON = $(VENV_DIR)\Scripts\python.exe
PIP = $(VENV_DIR)\Scripts\pip.exe

.PHONY: install clean

all: install
  
install: 
	rm -fr ./venv
	python3.12 -m venv ./venv
	./venv/bin/pip3 install -r requirements.txt
	 
run:
	venv/bin/uvicorn main:app --reload
.PHONY: install

all: install run

  
install:
ifeq ($(OS),Windows_NT)
	DEL /S /Q venv
	python -m venv venv
	venv\Scripts\pip install -r requirements.txt
else
	rm -fr ./venv
	python3.12 -m venv ./venv
	./venv/bin/pip install -r requirements.txt
endif
	 
run:
ifeq ($(OS),Windows_NT)
	venv\Scripts\uvicorn main:app --reload
else
	./venv/bin/uvicorn main:app --reload
endif

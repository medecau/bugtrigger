.PHONY: fmt
fmt:
	ruff check --fix .
	ruff format .

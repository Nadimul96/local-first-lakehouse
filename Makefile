.PHONY: demo test test-fast lint typecheck check bench report clean

demo:          ## Run the whole pipeline on synthetic data and write the HTML report
	uv run lakehouse demo

test:          ## All tests, including the subprocess kill matrix
	uv run pytest -q

test-fast:     ## Everything except the subprocess kill matrix
	uv run pytest -q -m "not slow"

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test

bench:         ## Measure throughput and overheads on this machine
	uv run lakehouse bench

clean:
	rm -rf .lakehouse .lakehouse-demo .pytest_cache .mypy_cache .ruff_cache .hypothesis

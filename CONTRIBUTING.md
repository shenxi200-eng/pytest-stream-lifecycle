# Contributing

Start with a reproducible failure and a bounded change. Include a regression
case that fails without the fix and passes with it. Do not replace real-network
coverage with mocks for network lifecycle behavior.

```sh
python -m pip install -e '.[dev]'
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m build
python -m twine check dist/*
```

AI-assisted contributions are welcome. Disclose material AI assistance, verify
all code and references, and never present generated test results as observed
runs. Keep credentials, private logs, and customer traffic out of issues and
fixtures. Synthetic applications should be small and run without remote APIs.

The public API is experimental. Discuss major scope expansion before building
it; HTTP/2, WebSockets, model evaluation, and general process supervision are
outside the initial scope.

# Flywheel CLI

**flywheel-cli** is a library and command line interface for interacting with a Flywheel site.

## Building

It's recommended that you use [pipenv](https://docs.pipenv.org/en/latest/) to manage dependencies. For example:
```
> python3 -m pip install pipenv
> pipenv --three install -e .[dev]
```

## Testing

Tests and python linting can be done using the standard tools:
```
> pipenv run pytest tests/unit_tests
> pipenv run pylint --rcfile .pylintrc flywheel_cli/
```

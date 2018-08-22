import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-e2e", action="store_true", default=False,
        help="run end to end tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-e2e"):
        # --run-e2e given in cli: do not skip e2e tests
        return
    skip_e2e = pytest.mark.skip(reason="need --run-e2e option to run")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_e2e)

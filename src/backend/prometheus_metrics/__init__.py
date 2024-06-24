from pathlib import Path
from prometheus_client import make_asgi_app, CollectorRegistry, multiprocess

import os
os.environ['PROMETHEUS_MULTIPROC_DIR'] = str(Path(__file__).parent / 'metrics')


# Using multiprocess collector for registry
def make_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)

from prometheus_client import make_asgi_app, CollectorRegistry, multiprocess


# Using multiprocess collector for registry
def make_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)

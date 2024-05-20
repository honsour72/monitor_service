from prometheus_client import start_http_server, Gauge
import random
import time

# Define gauges for the metrics
ariel1_gauge = Gauge('ariel1', "")
ariel2_gauge = Gauge('ariel2', "")
ariel3_gauge = Gauge('ariel3', "")

def update_metrics():
    """Update metrics with random values."""
    ariel1_gauge.set(random.randint(1, 15))
    ariel2_gauge.set(random.randint(1, 15))
    ariel3_gauge.set(random.randint(1, 15))

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Update metrics every second
    while True:
        update_metrics()
        time.sleep(1)

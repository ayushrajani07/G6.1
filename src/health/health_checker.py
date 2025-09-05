#!/usr/bin/env python3
"""
health/health_checker.py

Runs health checks for system components and records results in Prometheus metrics.
Supports per-index checks automatically from index_registry.
"""
import time
import logging
from typing import Callable, Optional
from monitoring.metrics import metrics_init, METRICS
from utils.index_registry import list_indices

logger = logging.getLogger(__name__)

def check_component(name: str, check_fn: Callable[[], bool], index: Optional[str] = None) -> None:
    """
    Run a health check for a given component and record metrics.

    Args:
        name: Component name (e.g., "broker_api", "redis", "influxdb").
        check_fn: Callable that returns True if healthy, False otherwise.
        index: Optional index symbol to tag the metric (e.g., "NIFTY").
    """
    metrics_init()
    start = time.time()
    status_metric = METRICS.get("health_check_status")
    duration_metric = METRICS.get("health_check_duration")

    try:
        healthy = bool(check_fn())
        if status_metric:
            status_metric.labels(component=name, index=index or "").set(1 if healthy else 0)
        logger.info(f"[HealthCheck] {name} {f'[{index}]' if index else ''} → {'healthy' if healthy else 'unhealthy'}")
    except Exception as e:
        if status_metric:
            status_metric.labels(component=name, index=index or "").set(0)
        logger.exception(f"[HealthCheck] {name} {f'[{index}]' if index else ''} → exception during check: {e}")
    finally:
        if duration_metric:
            duration_metric.labels(component=name, index=index or "").set(time.time() - start)

def check_all_indices(component_name: str, check_fn_factory: Callable[[str], Callable[[], bool]]) -> None:
    """
    Run a health check for every index in the registry.

    Args:
        component_name: Name of the component being checked.
        check_fn_factory: Function that takes an index symbol and returns a check_fn for that index.
    """
    for index in list_indices().keys():
        check_fn = check_fn_factory(index)
        check_component(component_name, check_fn, index=index)
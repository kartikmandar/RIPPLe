"""
Performance Monitoring and Benchmarking Utilities for RIPPLe

This module provides performance monitoring, timing utilities,
and benchmarking capabilities for the RIPPLe data access layer.
"""

import logging
import time
import functools
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class PerformanceMetric:
    """Data class for performance metrics."""
    operation_name: str
    start_time: float
    end_time: float
    duration: float
    success: bool
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class PerformanceMonitor:
    """
    Performance monitoring class for tracking data access operations.

    This class provides comprehensive timing and performance
    metrics collection for RIPPLe data access operations.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize performance monitor.

        Args:
            logger (logging.Logger, optional): Custom logger instance
        """
        self.metrics: List[PerformanceMetric] = []
        self.logger = logger or logging.getLogger(__name__)
        self.operation_counts: Dict[str, int] = {}

    def track_operation(self, operation_name: str):
        """
        Decorator to track operation performance.

        Args:
            operation_name (str): Name of operation to track

        Returns:
            function decorator for performance tracking
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                success = True
                error_message = None

                try:
                    result = func(*args, **kwargs)
                    return result

                except Exception as e:
                    success = False
                    error_message = str(e)
                    self.logger.error(f"Operation {operation_name} failed: {error_message}")
                    raise

                finally:
                    end_time = time.time()
                    duration = end_time - start_time

                    metric = PerformanceMetric(
                        operation_name=operation_name,
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        success=success,
                        error_message=error_message
                    )

                    self.metrics.append(metric)

                    # Update operation counts
                    self.operation_counts[operation_name] = self.operation_counts.get(operation_name, 0) + 1

                    self.logger.debug(f"Operation {operation_name} completed in {duration:.3f}s")

            return wrapper
        return decorator

    @contextmanager
    def measure_operation(self, operation_name: str, metadata: Dict[str, Any] = None):
        """
        Context manager to measure operation performance.

        Args:
            operation_name (str): Name of operation being measured
            metadata (Dict[str, Any], optional): Additional metadata to store

        Yields:
            None: Context manager
        """
        start_time = time.time()
        success = True
        error_message = None

        try:
            yield self

        except Exception as e:
            success = False
            error_message = str(e)
            self.logger.error(f"Operation {operation_name} failed: {error_message}")
            raise

        finally:
            end_time = time.time()
            duration = end_time - start_time

            metric = PerformanceMetric(
                operation_name=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                success=success,
                error_message=error_message,
                metadata=metadata
            )

            self.metrics.append(metric)
            self.operation_counts[operation_name] = self.operation_counts.get(operation_name, 0) + 1

            self.logger.debug(f"Operation {operation_name} completed in {duration:.3f}s")

    def get_performance_summary(self, operation_name: str = None) -> Dict[str, Any]:
        """
        Get performance summary for operations.

        Args:
            operation_name (str, optional): Filter by operation name

        Returns:
            Dict[str, Any]: Performance summary statistics
        """
        filtered_metrics = [m for m in self.metrics
                         if operation_name is None or m.operation_name == operation_name]

        if not filtered_metrics:
            return {"error": "No metrics available"}

        # Calculate statistics
        durations = [m.duration for m in filtered_metrics]
        successful_operations = [m for m in filtered_metrics if m.success]
        failed_operations = [m for m in filtered_metrics if not m.success]

        summary = {
            "total_operations": len(filtered_metrics),
            "successful_operations": len(successful_operations),
            "failed_operations": len(failed_operations),
            "success_rate": len(successful_operations) / len(filtered_metrics),
            "total_duration": sum(durations),
            "avg_duration": sum(durations) / len(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
            "std_duration": self._calculate_std(durations)
        }

        # Add operation-specific breakdown if no filter
        if operation_name is None:
            operation_breakdown = {}
            for op_name in set(m.operation_name for m in self.metrics):
                op_metrics = [m for m in self.metrics if m.operation_name == op_name]
                op_durations = [m.duration for m in op_metrics]
                op_successful = len([m for m in op_metrics if m.success])

                operation_breakdown[op_name] = {
                    "count": len(op_metrics),
                    "success_count": op_successful,
                    "success_rate": op_successful / len(op_metrics),
                    "avg_duration": sum(op_durations) / len(op_durations) if op_durations else 0,
                    "total_duration": sum(op_durations)
                }

            summary["operation_breakdown"] = operation_breakdown

        return summary

    def _calculate_std(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0

        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

    def get_slow_operations(self, threshold: float = 10.0,
                           limit: int = 10) -> List[PerformanceMetric]:
        """
        Get operations that were unusually slow.

        Args:
            threshold (float): Duration threshold in seconds
            limit (int): Maximum number of operations to return

        Returns:
            List[PerformanceMetric]: List of slow operations
        """
        slow_ops = [m for m in self.metrics
                   if m.duration > threshold and not m.success]

        # Sort by duration (slowest first)
        slow_ops.sort(key=lambda x: x.duration, reverse=True)

        return slow_ops[:limit]

    def export_metrics(self, filename: str, format: str = "json") -> bool:
        """
        Export performance metrics to file.

        Args:
            filename (str): Output filename
            format (str): Export format ('json', 'csv')

        Returns:
            bool: True if export successful
        """
        try:
            if format.lower() == "json":
                return self._export_json(filename)
            elif format.lower() == "csv":
                return self._export_csv(filename)
            else:
                self.logger.error(f"Unsupported export format: {format}")
                return False

        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")
            return False

    def _export_json(self, filename: str) -> bool:
        """Export metrics as JSON."""
        import json

        data = []
        for metric in self.metrics:
            data.append({
                "operation_name": metric.operation_name,
                "start_time": metric.start_time,
                "end_time": metric.end_time,
                "duration": metric.duration,
                "success": metric.success,
                "error_message": metric.error_message,
                "metadata": metric.metadata
            })

        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

        self.logger.info(f"Exported {len(data)} metrics to {filename}")
        return True

    def _export_csv(self, filename: str) -> bool:
        """Export metrics as CSV."""
        import csv

        fieldnames = ['operation_name', 'start_time', 'end_time', 'duration',
                     'success', 'error_message', 'metadata']

        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for metric in self.metrics:
                writer.writerow({
                    'operation_name': metric.operation_name,
                    'start_time': metric.start_time,
                    'end_time': metric.end_time,
                    'duration': metric.duration,
                    'success': metric.success,
                    'error_message': metric.error_message,
                    'metadata': str(metric.metadata) if metric.metadata else ''
                })

        self.logger.info(f"Exported {len(self.metrics)} metrics to {filename}")
        return True

    def clear_metrics(self) -> None:
        """Clear all stored metrics."""
        self.metrics.clear()
        self.operation_counts.clear()
        self.logger.info("Performance metrics cleared")

    def print_summary_report(self) -> None:
        """Print a formatted performance summary report."""
        summary = self.get_performance_summary()

        print("\n" + "="*60)
        print("RIPPLe PERFORMANCE SUMMARY")
        print("="*60)

        print(f"Total Operations: {summary['total_operations']}")
        print(f"Successful: {summary['successful_operations']}")
        print(f"Failed: {summary['failed_operations']}")
        print(f"Success Rate: {summary['success_rate']:.2%}")

        print(f"\nTiming Statistics:")
        print(f"  Total Duration: {summary['total_duration']:.2f}s")
        print(f"  Average Duration: {summary['avg_duration']:.3f}s")
        print(f"  Min Duration: {summary['min_duration']:.3f}s")
        print(f"  Max Duration: {summary['max_duration']:.3f}s")
        print(f"  Std Deviation: {summary['std_duration']:.3f}s")

        if 'operation_breakdown' in summary:
            print(f"\nOperation Breakdown:")
            for op_name, stats in summary['operation_breakdown'].items():
                print(f"  {op_name}:")
                print(f"    Count: {stats['count']}")
                print(f"    Success Rate: {stats['success_rate']:.2%}")
                print(f"    Avg Duration: {stats['avg_duration']:.3f}s")

        print("="*60)


# Global performance monitor instance
_global_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = PerformanceMonitor()
    return _global_monitor


def track_performance(operation_name: str):
    """Decorator to track operation performance using global monitor."""
    monitor = get_performance_monitor()
    return monitor.track_operation(operation_name)


def measure_performance(operation_name: str, metadata: Dict[str, Any] = None):
    """Context manager to measure operation performance using global monitor."""
    monitor = get_performance_monitor()
    return monitor.measure_operation(operation_name, metadata)


# Example usage:
#
# @track_performance("butler_get_calexp")
# def get_calibration_exposure(visit, detector):
#     # Function implementation
#     pass
#
# with measure_performance("coordinate_resolution", {"tract": 9813}):
#     # Perform coordinate resolution
#     pass
#
# # Get summary
# monitor = get_performance_monitor()
# monitor.print_summary_report()
#
# # Export metrics
# monitor.export_metrics("performance_metrics.json", "json")
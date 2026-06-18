"""Vendored third-party model architectures (provenance headers in each module).

Kept as a separate sub-package so RIPPLe never sys.path-injects the upstream
repos (which top-import matplotlib/PIL). Torch is imported lazily by the modules
themselves; importing this package is torch-free.
"""

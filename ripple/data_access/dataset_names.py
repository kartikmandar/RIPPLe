"""Release-aware Butler dataset-type name resolution.

LSST renamed several dataset types between DP0.2 and DP1
(``calexp`` -> ``visit_image``, ``deepCoadd`` -> ``deep_coadd``,
``objectTable`` -> ``object``). RIPPLe code refers to datasets by a stable
*logical key* and resolves the concrete Butler dataset type for the active
data release here, so the retrieval methods never hardcode a release-specific
string.

Logical keys:
    ``calexp`` - per-visit calibrated image (DP1: ``visit_image``)
    ``coadd``  - deep coadded image          (DP1: ``deep_coadd``)
    ``object`` - coadd object table          (DP1: ``object``)
    ``src``    - per-visit source catalog    (DP1: ``source``)
"""
from typing import Dict, Optional

DEFAULT_RELEASE = "dp1"

DATASET_NAMES: Dict[str, Dict[str, str]] = {
    "dp1": {
        "calexp": "visit_image",
        "coadd": "deep_coadd",
        "object": "object",
        "src": "source",
    },
    "dp02": {
        "calexp": "calexp",
        "coadd": "deepCoadd",
        "object": "objectTable",
        "src": "src",
    },
}

LOGICAL_KEYS = frozenset(DATASET_NAMES[DEFAULT_RELEASE])


def resolve_dataset_type(logical: str, release: str = DEFAULT_RELEASE,
                         overrides: Optional[Dict[str, str]] = None) -> str:
    """Resolve a logical dataset key to a concrete Butler dataset type.

    Precedence: ``overrides`` (per-key) > the release table > ``KeyError``.

    Args:
        logical: One of ``LOGICAL_KEYS`` (``calexp``/``coadd``/``object``/``src``).
        release: Data release key present in ``DATASET_NAMES`` (default ``dp1``).
        overrides: Optional per-key concrete-name overrides.

    Returns:
        The concrete Butler dataset type string.

    Raises:
        KeyError: If ``release`` or ``logical`` is unknown.
    """
    if overrides and logical in overrides:
        return overrides[logical]
    table = DATASET_NAMES[release]
    return table[logical]

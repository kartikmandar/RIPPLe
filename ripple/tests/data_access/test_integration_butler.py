"""Real end-to-end retrieval against pre-generated pipeline products.

Uses ``data/pipelines_check-29.1.1/DATA_REPO`` (collection ``demo_collection``),
which holds genuine LSST-pipeline HSC products with legacy naming, so the
client is configured with ``data_release="dp02"``. The whole module skips when
the LSST stack or the demo repo is unavailable, so a checkout without the stack
still collects cleanly.
"""
import os
import pytest

pytest.importorskip("lsst.daf.butler")
import lsst.afw.image as afwImage  # noqa: E402
from lsst.daf.butler import Butler  # noqa: E402
from ripple.data_access.butler_client import ButlerClient  # noqa: E402
from ripple.data_access.config_examples import ButlerConfig  # noqa: E402

REPO = "data/pipelines_check-29.1.1/DATA_REPO"
COLL = "demo_collection"

pytestmark = pytest.mark.skipif(
    not os.path.exists(REPO), reason=f"demo Butler repo not present at {REPO}"
)


@pytest.fixture(scope="module")
def calexp_dataid():
    """Discover a real (visit, detector) for a calexp in the demo collection."""
    butler = Butler(REPO, collections=[COLL])
    ref = next(iter(butler.registry.queryDatasets("calexp", collections=[COLL])))
    required = dict(ref.dataId.required)
    return int(required["visit"]), int(required["detector"])


@pytest.fixture(scope="module")
def client():
    config = ButlerConfig(repo_path=REPO, collections=[COLL], data_release="dp02")
    return ButlerClient(config=config)


def test_get_calexp_returns_real_exposure(client, calexp_dataid):
    visit, detector = calexp_dataid
    exposure = client.get_calexp(visit=visit, detector=detector)
    assert exposure is not None
    assert isinstance(exposure, afwImage.ExposureF)
    assert exposure.getDimensions().getX() > 0
    assert exposure.getDimensions().getY() > 0
    assert exposure.getInfo().getVisitInfo() is not None


def test_get_source_catalog_returns_real_catalog(client, calexp_dataid):
    visit, detector = calexp_dataid
    catalog = client.get_source_catalog(visit=visit, detector=detector)
    assert catalog is not None
    # a real afw SourceCatalog exposes a schema and supports len()
    assert hasattr(catalog, "getSchema")
    assert len(catalog) >= 0


def test_get_deepcoadd_missing_returns_none(client):
    # no deepCoadd exists in the demo repo -> graceful None (not-found path)
    assert client.get_deepCoadd(tract=0, patch=0, band="i") is None


def test_butler_client_resolves_legacy_names(client):
    assert client._dt("calexp") == "calexp"
    assert client._dt("coadd") == "deepCoadd"
    assert client._dt("src") == "src"

import hashlib
from pathlib import Path
import pytest

from cms_pricing.ingestion.services.conversion_factor_fetcher import (
    ConversionFactorFetcher,
)


class _DummyResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")


class _DummyAsyncClient:
    """
    Lightweight async client used to stub httpx.AsyncClient in tests.
    Records the requested URL and returns the configured response payload.
    """

    def __init__(self, *, content: bytes, status_code: int = 200, calls: list[str]):
        self._response = _DummyResponse(content=content, status_code=status_code)
        self._calls = calls

    async def __aenter__(self) -> "_DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    async def get(self, url: str) -> _DummyResponse:
        self._calls.append(url)
        return self._response


class _FailingAsyncClient:
    async def __aenter__(self) -> "_FailingAsyncClient":  # pragma: no cover - guard path
        raise AssertionError("AsyncClient should not be invoked when cache exists")

    async def __aexit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - guard path
        return None


def _patch_async_client(monkeypatch: pytest.MonkeyPatch, response_content: bytes, calls: list[str]) -> None:
    async_client_cls = lambda *args, **kwargs: _DummyAsyncClient(content=response_content, calls=calls)  # noqa: E731
    monkeypatch.setattr(
        "cms_pricing.ingestion.services.conversion_factor_fetcher.httpx.AsyncClient",
        async_client_cls,
    )


def _file_sha256(path: Path) -> str:
    sha256 = hashlib.sha256()
    with path.open("rb") as fh:
        sha256.update(fh.read())
    return sha256.hexdigest()


@pytest.mark.asyncio
async def test_ensure_conversion_factor_downloads_when_missing(monkeypatch, tmp_path):
    calls: list[str] = []
    payload = b"CY2025 MPFS Conversion Factor: 35.5"
    _patch_async_client(monkeypatch, payload, calls)

    fetcher = ConversionFactorFetcher(output_dir=str(tmp_path / "raw"))
    meta = await fetcher.ensure_conversion_factor(year=2025, source_url="https://cms.gov/cf.zip")

    assert calls == ["https://cms.gov/cf.zip"]
    downloaded_path = Path(meta.path)
    assert downloaded_path.exists()
    assert _file_sha256(downloaded_path) == meta.checksum
    assert meta.year == 2025


@pytest.mark.asyncio
async def test_ensure_conversion_factor_reuses_cache(monkeypatch, tmp_path):
    # Pre-create cached artefact
    cached_path = tmp_path / "raw" / "2025" / "cf.zip"
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    cached_path.write_text("cached conversion factor")

    # Patch AsyncClient to raise if called – ensures no download
    monkeypatch.setattr(
        "cms_pricing.ingestion.services.conversion_factor_fetcher.httpx.AsyncClient",
        lambda *args, **kwargs: _FailingAsyncClient(),  # noqa: E731
    )

    fetcher = ConversionFactorFetcher(output_dir=str(tmp_path / "raw"))
    meta = await fetcher.ensure_conversion_factor(year=2025, source_url="https://cms.gov/cf.zip")

    assert meta.path == str(cached_path)
    assert meta.checksum == _file_sha256(cached_path)


@pytest.mark.asyncio
async def test_ensure_conversion_factor_manual_override(tmp_path):
    override_path = tmp_path / "override" / "cf.txt"
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text("manual override")

    expected_checksum = _file_sha256(override_path)

    fetcher = ConversionFactorFetcher(output_dir=str(tmp_path / "raw"))
    meta = await fetcher.ensure_conversion_factor(
        year=2025,
        manual_override_path=str(override_path),
        expected_checksum=expected_checksum,
    )

    assert meta.path == str(override_path)
    assert meta.checksum == expected_checksum
    assert meta.source_url.startswith("file://")

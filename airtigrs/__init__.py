"""Top-level package for airtigrs"""

try:
    from ._version import __version__
except ModuleNotFoundError:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution("airtigrs").version
    except DistributionNotFound:
        __version__ = "unknown"
    del get_distribution
    del DistributionNotFound

__packagename__ = "airtigrs"
__url__ = "https://github.com/tigrlab/air-tigrs"

_all__ = [
    "__packagename__",
    "__version__",
]

DOWNLOAD_URL = (
    f"https://github.com/tigrlab/{__packagename__}/archive/{__version__}.tar.gz"
)

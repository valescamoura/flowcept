"""Galaxy adapter dataclasses module."""

from dataclasses import dataclass
from typing import Union

from flowcept.commons.flowcept_dataclasses.base_settings_dataclasses import (
    BaseSettings,
)


@dataclass
class GalaxySettings(BaseSettings):
    """Galaxy settings.

    ``url`` and ``api_key`` identify the Galaxy instance and the account whose
    workflow invocations are observed. ``verify`` is passed to the HTTP client
    (``True``, ``False`` or the path of a CA bundle). ``state_path`` is the JSON
    file that keeps the ids of the invocations already intercepted.
    """

    key: str = "galaxy"
    kind: str = "galaxy"
    url: str = None
    api_key: str = None
    verify: Union[bool, str] = True
    watch_interval_sec: int = 30
    invocations_limit: int = 100
    state_path: str = "galaxy_interceptor_state.json"

    def __post_init__(self):
        """Set attributes after init."""
        self.observer_type = "web_api"
        self.observer_subtype = "galaxy"

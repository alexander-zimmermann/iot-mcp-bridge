"""Single source of truth for anomaly use-cases.

Each UC owns a stable slug — the slug becomes part of the NATS subject
(`anomaly.<slug>.<severity>`) which the knx-nats-bridge write-mapping
resolves to a KNX-GA. Once published, do NOT rename a slug; the mapping
ConfigMap and Basalte notification rules pin to it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UnivariateMetric:
    """A single (CAGG, column) pair scored by robust z-score detector."""

    uc: str
    source_cagg: str
    metric: str
    severity_floor: str = "info"
    warmup_days: int = 7
    silenced: bool = False


@dataclass(frozen=True)
class IsolationForestUseCase:
    """Multivariate features fit by IsolationForest, scored hourly."""

    uc: str
    source_cagg: str
    feature_cols: tuple[str, ...]
    lookback_days: int = 60
    contamination: float = 0.005
    severity_floor: str = "info"
    warmup_days: int = 7
    silenced: bool = False


@dataclass(frozen=True)
class SeasonalModel:
    """statsforecast MSTL+AutoARIMA target metric."""

    uc: str
    source_cagg: str
    metric: str
    season_length: tuple[int, ...] = (24, 168)
    lookback_days: int = 365
    severity_floor: str = "info"
    warmup_days: int = 14
    silenced: bool = False


# Populated by the detector implementations when each lands.
UNIVARIATE_METRICS: tuple[UnivariateMetric, ...] = ()
IFOREST_USECASES: tuple[IsolationForestUseCase, ...] = ()
SEASONAL_MODELS: tuple[SeasonalModel, ...] = ()


def all_slugs() -> set[str]:
    return (
        {m.uc for m in UNIVARIATE_METRICS}
        | {u.uc for u in IFOREST_USECASES}
        | {s.uc for s in SEASONAL_MODELS}
    )

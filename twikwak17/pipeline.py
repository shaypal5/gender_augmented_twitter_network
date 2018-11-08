"""Running the entire data generation pipeline."""

from .phases import (
    phase1,
)


def run_pipeline(source_dpath):
    """Runs the entire data generation pipeline."""
    phase1(source_dpath)

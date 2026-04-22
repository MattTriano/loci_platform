"""
Boto3 session helper that respects per-env AWS profile configuration.

Using a Session lets us select the SSO profile explicitly from EnvConfig
rather than relying on ambient environment state, so the env param on the
DAG is the single source of truth for which AWS account we target.
"""

from __future__ import annotations

import boto3
from loci.environments import EnvConfig


def get_boto_session(cfg: EnvConfig) -> boto3.Session:
    """Return a boto3 Session for the given env.

    If cfg.aws_profile is set, uses that profile. Otherwise falls back to
    whatever boto3 picks up from the ambient environment (env vars,
    ~/.aws/credentials, instance role, etc.).
    """
    kwargs: dict = {"region_name": cfg.aws_region}
    if cfg.aws_profile:
        kwargs["profile_name"] = cfg.aws_profile
    return boto3.Session(**kwargs)

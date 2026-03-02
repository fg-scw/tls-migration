"""Configuration models for vmware2scw using Pydantic v2."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class VMwareConfig(BaseModel):
    """VMware vCenter/vSphere connection configuration."""

    vcenter: str = Field(..., description="vCenter hostname or IP")
    username: str = Field(..., description="vCenter username")
    password: Optional[SecretStr] = Field(None, description="vCenter password (prefer password_env)")
    password_env: Optional[str] = Field(None, description="Environment variable containing the password")
    insecure: bool = Field(False, description="Skip SSL certificate verification")
    port: int = Field(443, description="vCenter port")

    @model_validator(mode="after")
    def resolve_password(self) -> "VMwareConfig":
        if self.password is None and self.password_env:
            env_val = os.environ.get(self.password_env)
            if env_val:
                self.password = SecretStr(env_val)
        if self.password is None:
            raise ValueError("Either 'password' or 'password_env' (with matching env var) must be provided")
        return self


class ScalewayConfig(BaseModel):
    """Scaleway API and S3 configuration."""

    access_key: Optional[str] = Field(None)
    access_key_env: Optional[str] = Field("SCW_ACCESS_KEY")
    secret_key: Optional[SecretStr] = Field(None)
    secret_key_env: Optional[str] = Field("SCW_SECRET_KEY")
    organization_id: str = Field(..., description="Scaleway Organization ID")
    project_id: str = Field(..., description="Scaleway Project ID")
    default_zone: str = Field("fr-par-1", description="Default availability zone")
    s3_region: str = Field("fr-par", description="S3 region for object storage")
    s3_bucket: str = Field("vmware-migration-transit", description="S3 bucket for transit images")

    @model_validator(mode="after")
    def resolve_credentials(self) -> "ScalewayConfig":
        if self.access_key is None and self.access_key_env:
            self.access_key = os.environ.get(self.access_key_env)
        if self.secret_key is None and self.secret_key_env:
            env_val = os.environ.get(self.secret_key_env)
            if env_val:
                self.secret_key = SecretStr(env_val)
        if not self.access_key:
            raise ValueError("Scaleway access_key not found (check SCW_ACCESS_KEY env var)")
        if not self.secret_key:
            raise ValueError("Scaleway secret_key not found (check SCW_SECRET_KEY env var)")
        return self


class ConversionConfig(BaseModel):
    """Disk conversion settings."""

    work_dir: Path = Field(Path("/var/lib/vmware2scw/work"), description="Working directory for temp files")
    compress_qcow2: bool = Field(True, description="Compress qcow2 output (slower but smaller)")
    virtio_win_iso: Optional[Path] = Field(None, description="Path to virtio-win.iso for Windows VMs")
    cleanup_on_success: bool = Field(True, description="Remove temp files after successful migration")
    virt_v2v_verbose: bool = Field(False, description="Enable verbose virt-v2v output")

    @field_validator("work_dir")
    @classmethod
    def ensure_work_dir(cls, v: Path) -> Path:
        v.mkdir(parents=True, exist_ok=True)
        return v


class MigrationSettings(BaseModel):
    """Global migration behavior settings."""

    parallel_exports: int = Field(2, ge=1, le=10, description="Max parallel VMDK exports")
    parallel_uploads: int = Field(3, ge=1, le=10, description="Max parallel S3 uploads")
    retry_count: int = Field(3, ge=0, le=10, description="Retry count for transient errors")
    retry_delay_seconds: int = Field(30, ge=5, description="Base delay between retries")
    export_strategy: str = Field("local", pattern="^(local|streaming)$", description="Export strategy: local or streaming (NBD)")


class AppConfig(BaseModel):
    """Root application configuration."""

    vmware: VMwareConfig
    scaleway: ScalewayConfig
    conversion: ConversionConfig = ConversionConfig()
    migration: MigrationSettings = MigrationSettings()

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AppConfig":
        """Load configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    @classmethod
    def from_env_and_args(cls, **overrides) -> "AppConfig":
        """Build config from environment variables with CLI overrides."""
        base = {
            "vmware": {
                "vcenter": os.environ.get("VCENTER_HOST", ""),
                "username": os.environ.get("VCENTER_USERNAME", ""),
                "password_env": "VCENTER_PASSWORD",
                "insecure": os.environ.get("VCENTER_INSECURE", "false").lower() == "true",
            },
            "scaleway": {
                "organization_id": os.environ.get("SCW_ORGANIZATION_ID", ""),
                "project_id": os.environ.get("SCW_PROJECT_ID", ""),
                "default_zone": os.environ.get("SCW_DEFAULT_ZONE", "fr-par-1"),
                "s3_region": os.environ.get("SCW_S3_REGION", "fr-par"),
                "s3_bucket": os.environ.get("SCW_S3_BUCKET", "vmware-migration-transit"),
            },
        }
        # Deep merge overrides
        for key, value in overrides.items():
            if isinstance(value, dict) and key in base:
                base[key].update(value)
            else:
                base[key] = value
        return cls(**base)


# --- VM-specific migration config ---

class VMMigrationPlan(BaseModel):
    """Migration plan for a single VM."""

    vm_name: str = Field(..., description="Source VM name in vCenter")
    target_type: str = Field(..., description="Scaleway instance type (e.g. PRO2-S)")
    zone: str = Field("fr-par-1", description="Target Scaleway zone")
    network_mapping: dict[str, str] = Field(default_factory=dict, description="VMware network → SCW VPC mapping")
    tags: list[str] = Field(default_factory=list, description="Tags for the Scaleway instance")
    priority: int = Field(10, description="Migration priority (lower = higher priority)")
    skip_validation: bool = Field(False, description="Skip pre-migration validation (not recommended)")


class BatchMigrationPlan(BaseModel):
    """Batch migration plan with multiple VMs."""

    migrations: list[VMMigrationPlan] = Field(..., min_length=1)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "BatchMigrationPlan":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    def sorted_by_priority(self) -> list[VMMigrationPlan]:
        return sorted(self.migrations, key=lambda m: m.priority)

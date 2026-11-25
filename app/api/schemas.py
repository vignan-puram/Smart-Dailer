"""Pydantic request models (input validation for the demo API)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class CreateAgents(BaseModel):
    count: int = Field(1, ge=1, le=20_000)
    prefix: str = Field("agent", pattern=r"^[a-zA-Z0-9_-]{1,20}$")
    status: Literal["AVAILABLE", "OFFLINE"] = "AVAILABLE"


class AgentStatusChange(BaseModel):
    status: Literal["AVAILABLE", "PAUSED", "OFFLINE"]


class CreateCampaign(BaseModel):
    id: str = Field(..., pattern=r"^[a-zA-Z0-9_-]{1,40}$")
    name: str = Field(..., min_length=1, max_length=100)
    mode: Literal["PROGRESSIVE", "PREDICTIVE"] = "PROGRESSIVE"
    max_attempts: int = Field(3, ge=1, le=10)
    retry_delay_s: float = Field(300.0, ge=0, le=86_400)
    max_concurrent_calls: int = Field(1000, ge=1, le=100_000)


class CreateBorrowers(BaseModel):
    count: int = Field(100, ge=1, le=200_000)


class SetMode(BaseModel):
    mode: Literal["PROGRESSIVE", "PREDICTIVE"]


class AdvanceTime(BaseModel):
    seconds: float = Field(..., gt=0, le=7200)
    dial: bool = True


class Outage(BaseModel):
    active: bool
    webhooks: bool = False


class RunSimulation(BaseModel):
    scenario: Literal["A", "B", "C", "D"] = "A"
    mode: Literal["PROGRESSIVE", "PREDICTIVE"] = "PREDICTIVE"
    safety_mode: Literal["STRICT", "BOUNDED_RISK"] = "STRICT"
    duration_s: Optional[float] = Field(600.0, gt=0, le=4800)
    seed: int = 42

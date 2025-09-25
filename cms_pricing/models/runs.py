"""Run tracking and trace models for auditability"""

from sqlalchemy import Column, String, Integer, Date, Text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from cms_pricing.database import Base
import uuid


class Run(Base):
    """Pricing run tracking"""
    
    __tablename__ = "runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(String(50), nullable=False, unique=True, index=True)
    endpoint = Column(String(50), nullable=False)  # /price, /compare, etc.
    request_json = Column(JSONB, nullable=True)  # Full request payload
    response_json = Column(JSONB, nullable=True)  # Full response payload
    status = Column(String(20), nullable=False, default="success")  # success, error, partial
    created_at = Column(Date, nullable=False)
    duration_ms = Column(Integer, nullable=True)
    
    # Relationships
    inputs = relationship("RunInput", back_populates="run", cascade="all, delete-orphan")
    outputs = relationship("RunOutput", back_populates="run", cascade="all, delete-orphan")
    traces = relationship("RunTrace", back_populates="run", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index("idx_runs_run_id", "run_id"),
        Index("idx_runs_created_at", "created_at"),
    )


class RunInput(Base):
    """Input parameters for a pricing run"""
    
    __tablename__ = "run_inputs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, index=True)
    parameter_name = Column(String(50), nullable=False)
    parameter_value = Column(Text, nullable=True)
    parameter_type = Column(String(20), nullable=True)  # string, number, boolean, json
    
    # Relationships
    run = relationship("Run", back_populates="inputs")


class RunOutput(Base):
    """Output results for a pricing run"""
    
    __tablename__ = "run_outputs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, index=True)
    line_sequence = Column(Integer, nullable=True)  # For line-item outputs
    code = Column(String(5), nullable=True)
    setting = Column(String(20), nullable=True)
    allowed_cents = Column(Integer, nullable=True)
    beneficiary_deductible_cents = Column(Integer, nullable=True)
    beneficiary_coinsurance_cents = Column(Integer, nullable=True)
    beneficiary_total_cents = Column(Integer, nullable=True)
    program_payment_cents = Column(Integer, nullable=True)
    source = Column(String(20), nullable=True)  # benchmark, mrf, tic
    trace_refs = Column(JSONB, nullable=True)
    
    # Relationships
    run = relationship("Run", back_populates="outputs")


class RunTrace(Base):
    """Detailed trace information for auditability"""
    
    __tablename__ = "run_trace"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, index=True)
    trace_type = Column(String(50), nullable=False)  # dataset_selection, geo_resolution, formula, etc.
    trace_data = Column(JSONB, nullable=False)  # Structured trace data
    line_sequence = Column(Integer, nullable=True)  # For line-specific traces
    
    # Relationships
    run = relationship("Run", back_populates="traces")
    
    # Indexes
    __table_args__ = (
        Index("idx_run_trace_run_type", "run_id", "trace_type"),
    )

"""Benefit parameters and cost sharing rules"""

from sqlalchemy import Column, String, Integer, Float, Date, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class BenefitParams(Base):
    """Benefit parameters for cost sharing calculations"""
    
    __tablename__ = "benefit_params"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    setting = Column(String(20), nullable=False, index=True)  # PartA, PartB, OPPS, etc.
    rules_json = Column(Text, nullable=False)  # JSON with deductibles, coinsurance rates, caps
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_benefit_params_year_setting", "year", "setting"),
    )

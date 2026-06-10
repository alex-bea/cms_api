"""Base pricing engine"""

from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from cms_pricing.schemas.pricing import CodePricingItem

from cms_pricing.schemas.geography import GeographyResolveResponse


class BasePricingEngine(ABC):
    """Base class for all pricing engines"""
    
    @abstractmethod
    async def price_code(
        self,
        code: str,
        zip: str,
        year: int,
        quarter: Optional[str] = None,
        geography: Optional[GeographyResolveResponse] = None,
        ccn: Optional[str] = None,
        payer: Optional[str] = None,
        plan: Optional[str] = None,
        units: float = 1.0,
        utilization_weight: float = 1.0,
        professional_component: bool = True,
        facility_component: bool = True,
        modifiers: Optional[List[str]] = None,
        pos: Optional[str] = None,
        ndc11: Optional[str] = None
    ) -> "CodePricingItem":
        """Price a single code/component (Quick Win #2: Returns unified CodePricingItem)"""
        pass
    
    @staticmethod
    def _normalize_modifier_code(modifier: Any) -> str:
        """Normalize modifier input to a bare uppercase code."""
        return str(modifier).strip().upper().lstrip("-")

    def _apply_modifiers(self, base_amount: float, modifiers: List[str]) -> float:
        """Apply modifiers to base amount"""
        amount = base_amount
        
        for modifier in modifiers:
            modifier_code = self._normalize_modifier_code(modifier)
            if modifier_code == "50":  # Bilateral
                amount *= 1.5
            elif modifier_code == "51":  # Multiple procedures
                amount *= 0.5
            # Add other modifier logic as needed
        
        return amount
    
    def _calculate_beneficiary_cost_sharing(
        self,
        allowed_amount: float,
        deductible_remaining: float = 0.0,
        coinsurance_rate: float = 0.20
    ) -> Dict[str, float]:
        """Calculate beneficiary cost sharing"""
        
        # Apply deductible
        deductible_applied = min(deductible_remaining, allowed_amount)
        remaining_after_deductible = allowed_amount - deductible_applied
        
        # Calculate coinsurance
        coinsurance = remaining_after_deductible * coinsurance_rate
        
        # Total beneficiary cost
        beneficiary_total = deductible_applied + coinsurance
        
        # Program payment
        program_payment = allowed_amount - beneficiary_total
        
        return {
            "beneficiary_deductible": deductible_applied,
            "beneficiary_coinsurance": coinsurance,
            "beneficiary_total": beneficiary_total,
            "program_payment": program_payment,
            "remaining_deductible": deductible_remaining - deductible_applied
        }

    def _amount_to_cents(self, amount: float) -> int:
        """Round a dollar amount to integer cents."""
        return int(
            (Decimal(str(amount)) * Decimal("100")).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )
        )

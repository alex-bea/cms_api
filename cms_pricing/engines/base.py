"""Base pricing engine"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
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
    ) -> Dict[str, Any]:
        """Price a single code/component"""
        pass
    
    def _apply_modifiers(self, base_amount: float, modifiers: List[str]) -> float:
        """Apply modifiers to base amount"""
        amount = base_amount
        
        for modifier in modifiers:
            if modifier == "-50":  # Bilateral
                amount *= 1.5
            elif modifier == "-51":  # Multiple procedures
                amount *= 0.5
            # Add other modifier logic as needed
        
        return amount
    
    def _calculate_beneficiary_cost_sharing(
        self,
        allowed_amount: float,
        deductible_remaining: float,
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

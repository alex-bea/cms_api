"""Trace and audit service"""

import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime

from cms_pricing.schemas.trace import TraceResponse, TraceData
from cms_pricing.database import SessionLocal
from cms_pricing.models.runs import Run, RunInput, RunOutput, RunTrace
import structlog

logger = structlog.get_logger()


class TraceService:
    """Service for managing run traces and auditability"""
    
    def __init__(self):
        self.db = SessionLocal()
    
    async def store_run(
        self,
        run_id: str,
        endpoint: str,
        request_data: Dict[str, Any],
        response_data: Optional[Dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None
    ) -> str:
        """Store a pricing run with full trace information"""
        
        try:
            # Create run record
            run = Run(
                run_id=run_id,
                endpoint=endpoint,
                request_json=request_data,
                response_json=response_data,
                status=status,
                created_at=datetime.utcnow(),
                duration_ms=duration_ms
            )
            
            self.db.add(run)
            self.db.flush()
            
            # Store input parameters
            for key, value in request_data.items():
                input_record = RunInput(
                    run_id=run.id,
                    parameter_name=key,
                    parameter_value=str(value) if value is not None else None,
                    parameter_type=type(value).__name__
                )
                self.db.add(input_record)
            
            # Store output results if available
            if response_data and 'line_items' in response_data:
                for i, line_item in enumerate(response_data['line_items']):
                    output_record = RunOutput(
                        run_id=run.id,
                        line_sequence=i + 1,
                        code=line_item.get('code'),
                        setting=line_item.get('setting'),
                        allowed_cents=line_item.get('allowed_cents'),
                        beneficiary_deductible_cents=line_item.get('beneficiary_deductible_cents'),
                        beneficiary_coinsurance_cents=line_item.get('beneficiary_coinsurance_cents'),
                        beneficiary_total_cents=line_item.get('beneficiary_total_cents'),
                        program_payment_cents=line_item.get('program_payment_cents'),
                        source=line_item.get('source'),
                        trace_refs=line_item.get('trace_refs')
                    )
                    self.db.add(output_record)
            
            # Store trace data
            trace_data = TraceData(
                trace_type="run_summary",
                trace_data={
                    "endpoint": endpoint,
                    "status": status,
                    "duration_ms": duration_ms,
                    "error_message": error_message,
                    "request_keys": list(request_data.keys()),
                    "response_keys": list(response_data.keys()) if response_data else []
                }
            )
            
            trace_record = RunTrace(
                run_id=run.id,
                trace_type=trace_data.trace_type,
                trace_data=trace_data.trace_data,
                timestamp=trace_data.timestamp
            )
            self.db.add(trace_record)
            
            self.db.commit()
            
            logger.info(
                "Run stored successfully",
                run_id=run_id,
                endpoint=endpoint,
                status=status
            )
            
            return run_id
            
        except Exception as e:
            self.db.rollback()
            logger.error(
                "Failed to store run",
                run_id=run_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def get_trace(self, run_id: str) -> Optional[TraceResponse]:
        """Get full trace information for a run"""
        
        try:
            # Get run record
            run = self.db.query(Run).filter(Run.run_id == run_id).first()
            if not run:
                return None
            
            # Get trace records
            traces = self.db.query(RunTrace).filter(RunTrace.run_id == run.id).all()
            
            # Convert to trace data
            trace_data = []
            for trace in traces:
                trace_data.append(TraceData(
                    trace_type=trace.trace_type,
                    trace_data=trace.trace_data,
                    line_sequence=trace.line_sequence,
                    timestamp=trace.timestamp
                ))
            
            # Create response
            response = TraceResponse(
                run_id=run.run_id,
                endpoint=run.endpoint,
                status=run.status,
                created_at=run.created_at,
                duration_ms=run.duration_ms,
                request_json=run.request_json,
                response_json=run.response_json,
                traces=trace_data,
                datasets_used=[],  # TODO: Extract from traces
                cache_hits=0,  # TODO: Extract from traces
                cache_misses=0,  # TODO: Extract from traces
                facility_rates_used=0,  # TODO: Extract from traces
                benchmark_rates_used=0  # TODO: Extract from traces
            )
            
            return response
            
        except Exception as e:
            logger.error(
                "Failed to get trace",
                run_id=run_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def replay_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Replay a pricing run with identical parameters"""
        
        try:
            # Get original run
            run = self.db.query(Run).filter(Run.run_id == run_id).first()
            if not run:
                return None
            
            if not run.request_json:
                logger.warning("Cannot replay run without request data", run_id=run_id)
                return None
            
            # TODO: Implement actual replay logic
            # This would involve:
            # 1. Extracting the original request parameters
            # 2. Calling the appropriate pricing service
            # 3. Comparing results for consistency
            
            logger.info("Run replay requested", run_id=run_id)
            
            return {
                "message": "Replay functionality not yet implemented",
                "original_run_id": run_id,
                "replay_run_id": str(uuid.uuid4())
            }
            
        except Exception as e:
            logger.error(
                "Failed to replay run",
                run_id=run_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()

"""
Query analysis and optimization tools

Analyzes query patterns and suggests optimizations
"""

import time
from typing import Dict, List, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from cms_pricing.database import SessionLocal
import logging

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """Analyzes and optimizes database queries"""
    
    def __init__(self):
        self.db = SessionLocal()
    
    def analyze_query_patterns(self) -> Dict[str, Any]:
        """Analyze common query patterns and their performance"""
        
        print("🔍 Analyzing query patterns...")
        
        # Define common query patterns
        patterns = {
            'hcpcs_lookup': {
                'description': 'Lookup RVU data by HCPCS code',
                'queries': [
                    "SELECT * FROM rvu_items WHERE hcpcs_code = '99213' AND effective_start <= CURRENT_DATE AND effective_end >= CURRENT_DATE",
                    "SELECT * FROM rvu_items WHERE hcpcs_code LIKE '99%' AND status_code = 'A'",
                    "SELECT hcpcs_code, work_rvu, pe_rvu_nonfac FROM rvu_items WHERE hcpcs_code = '99213'"
                ]
            },
            'status_filtering': {
                'description': 'Filter by status codes',
                'queries': [
                    "SELECT * FROM rvu_items WHERE status_code IN ('A', 'R', 'T') AND effective_start <= CURRENT_DATE",
                    "SELECT * FROM rvu_items WHERE status_code = 'A' AND work_rvu > 0",
                    "SELECT COUNT(*) FROM rvu_items WHERE status_code = 'I'"
                ]
            },
            'date_range_queries': {
                'description': 'Queries with date range filtering',
                'queries': [
                    "SELECT * FROM rvu_items WHERE effective_start >= CURRENT_DATE - INTERVAL '30 days'",
                    "SELECT * FROM rvu_items WHERE effective_start BETWEEN '2025-01-01' AND '2025-12-31'",
                    "SELECT * FROM rvu_items WHERE effective_end >= CURRENT_DATE"
                ]
            },
            'gpci_lookups': {
                'description': 'GPCI data lookups',
                'queries': [
                    "SELECT * FROM gpci_indices WHERE mac = '10112' AND locality_id = '00'",
                    "SELECT * FROM gpci_indices WHERE state = 'CA' AND effective_start <= CURRENT_DATE",
                    "SELECT work_gpci, pe_gpci, mp_gpci FROM gpci_indices WHERE mac = '10112'"
                ]
            },
            'join_queries': {
                'description': 'Complex join operations',
                'queries': [
                    "SELECT r.hcpcs_code, r.work_rvu, g.work_gpci FROM rvu_items r JOIN gpci_indices g ON r.release_id = g.release_id",
                    "SELECT r.hcpcs_code, o.price_fac FROM rvu_items r JOIN opps_caps o ON r.hcpcs_code = o.hcpcs_code",
                    "SELECT r.hcpcs_code, a.anesthesia_cf FROM rvu_items r JOIN anes_cfs a ON r.release_id = a.release_id"
                ]
            }
        }
        
        results = {}
        
        for pattern_name, pattern_data in patterns.items():
            print(f"   Analyzing {pattern_name}...")
            
            pattern_results = {
                'description': pattern_data['description'],
                'queries': []
            }
            
            for i, query_sql in enumerate(pattern_data['queries']):
                try:
                    # Measure execution time
                    start_time = time.time()
                    result = self.db.execute(text(query_sql))
                    rows = result.fetchall()
                    end_time = time.time()
                    
                    execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
                    
                    # Get query plan
                    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query_sql}"
                    plan_result = self.db.execute(text(explain_sql))
                    plan = plan_result.fetchone()[0][0]
                    
                    query_result = {
                        'sql': query_sql,
                        'execution_time_ms': execution_time,
                        'rows_returned': len(rows),
                        'plan': plan,
                        'total_cost': plan['Plan']['Total Cost'],
                        'execution_time_db': plan['Execution Time'],
                        'planning_time': plan['Planning Time']
                    }
                    
                    pattern_results['queries'].append(query_result)
                    
                    print(f"     Query {i+1}: {execution_time:.2f}ms ({len(rows)} rows)")
                    
                except Exception as e:
                    logger.error(f"Failed to analyze query {pattern_name}.{i+1}: {e}")
                    pattern_results['queries'].append({
                        'sql': query_sql,
                        'error': str(e)
                    })
            
            results[pattern_name] = pattern_results
        
        return results
    
    def identify_slow_queries(self, analysis_results: Dict[str, Any], threshold_ms: float = 100.0) -> List[Dict[str, Any]]:
        """Identify queries that exceed performance threshold"""
        
        slow_queries = []
        
        for pattern_name, pattern_data in analysis_results.items():
            for i, query_data in enumerate(pattern_data['queries']):
                if 'execution_time_ms' in query_data and query_data['execution_time_ms'] > threshold_ms:
                    slow_queries.append({
                        'pattern': pattern_name,
                        'query_index': i,
                        'execution_time_ms': query_data['execution_time_ms'],
                        'sql': query_data['sql'],
                        'rows_returned': query_data.get('rows_returned', 0),
                        'total_cost': query_data.get('total_cost', 0)
                    })
        
        return slow_queries
    
    def suggest_optimizations(self, analysis_results: Dict[str, Any]) -> Dict[str, List[str]]:
        """Suggest optimizations based on query analysis"""
        
        suggestions = {}
        
        for pattern_name, pattern_data in analysis_results.items():
            pattern_suggestions = []
            
            for query_data in pattern_data['queries']:
                if 'error' in query_data:
                    continue
                
                sql = query_data['sql']
                execution_time = query_data.get('execution_time_ms', 0)
                plan = query_data.get('plan', {})
                
                # Analyze query plan for optimization opportunities
                if 'Plan' in plan:
                    plan_info = plan['Plan']
                    
                    # Check for sequential scans
                    if self._has_sequential_scan(plan_info):
                        pattern_suggestions.append("Consider adding an index to avoid sequential scan")
                    
                    # Check for nested loops
                    if self._has_nested_loop(plan_info):
                        pattern_suggestions.append("Consider optimizing join conditions or adding indexes")
                    
                    # Check for high cost operations
                    if plan_info.get('Total Cost', 0) > 1000:
                        pattern_suggestions.append("High cost query - consider rewriting or adding indexes")
                
                # Check execution time
                if execution_time > 500:  # 500ms threshold
                    pattern_suggestions.append("Query exceeds 500ms threshold - needs optimization")
                elif execution_time > 100:  # 100ms threshold
                    pattern_suggestions.append("Query is slow - consider optimization")
                
                # Check for missing WHERE clauses
                if 'WHERE' not in sql.upper() and 'SELECT' in sql.upper():
                    pattern_suggestions.append("Query lacks WHERE clause - may return too many rows")
                
                # Check for SELECT * usage
                if 'SELECT *' in sql.upper():
                    pattern_suggestions.append("Consider selecting only needed columns instead of *")
            
            if pattern_suggestions:
                suggestions[pattern_name] = list(set(pattern_suggestions))  # Remove duplicates
        
        return suggestions
    
    def _has_sequential_scan(self, plan_info: Dict[str, Any]) -> bool:
        """Check if query plan includes sequential scan"""
        
        if plan_info.get('Node Type') == 'Seq Scan':
            return True
        
        if 'Plans' in plan_info:
            for sub_plan in plan_info['Plans']:
                if self._has_sequential_scan(sub_plan):
                    return True
        
        return False
    
    def _has_nested_loop(self, plan_info: Dict[str, Any]) -> bool:
        """Check if query plan includes nested loop"""
        
        if plan_info.get('Node Type') == 'Nested Loop':
            return True
        
        if 'Plans' in plan_info:
            for sub_plan in plan_info['Plans']:
                if self._has_nested_loop(sub_plan):
                    return True
        
        return False
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance analysis report"""
        
        print("📊 Generating performance analysis report...")
        
        # Analyze query patterns
        analysis_results = self.analyze_query_patterns()
        
        # Identify slow queries
        slow_queries = self.identify_slow_queries(analysis_results)
        
        # Get optimization suggestions
        suggestions = self.suggest_optimizations(analysis_results)
        
        # Calculate summary statistics
        total_queries = sum(len(pattern['queries']) for pattern in analysis_results.values())
        successful_queries = sum(
            len([q for q in pattern['queries'] if 'error' not in q])
            for pattern in analysis_results.values()
        )
        failed_queries = total_queries - successful_queries
        
        # Calculate average execution times
        execution_times = []
        for pattern_data in analysis_results.values():
            for query_data in pattern_data['queries']:
                if 'execution_time_ms' in query_data:
                    execution_times.append(query_data['execution_time_ms'])
        
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
        max_execution_time = max(execution_times) if execution_times else 0
        
        report = {
            'summary': {
                'total_queries': total_queries,
                'successful_queries': successful_queries,
                'failed_queries': failed_queries,
                'slow_queries_count': len(slow_queries),
                'avg_execution_time_ms': avg_execution_time,
                'max_execution_time_ms': max_execution_time
            },
            'analysis_results': analysis_results,
            'slow_queries': slow_queries,
            'suggestions': suggestions
        }
        
        print(f"   Total queries analyzed: {total_queries}")
        print(f"   Successful queries: {successful_queries}")
        print(f"   Failed queries: {failed_queries}")
        print(f"   Slow queries: {len(slow_queries)}")
        print(f"   Average execution time: {avg_execution_time:.2f}ms")
        print(f"   Max execution time: {max_execution_time:.2f}ms")
        
        return report
    
    def close(self):
        """Clean up resources"""
        self.db.close()




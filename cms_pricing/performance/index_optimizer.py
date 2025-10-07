"""
Database index optimization for RVU data

Analyzes query patterns and creates optimal indices for performance
"""

from typing import List, Dict, Any
from sqlalchemy import text, Index
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal, engine
from cms_pricing.models.rvu import RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty, Release
import logging

logger = logging.getLogger(__name__)


class IndexOptimizer:
    """Optimizes database indices for RVU data performance"""
    
    def __init__(self):
        self.db = SessionLocal()
    
    def analyze_existing_indices(self) -> Dict[str, List[str]]:
        """Analyze existing indices in the database"""
        
        print("🔍 Analyzing existing indices...")
        
        # Query to get all indices
        query = text("""
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes 
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        
        result = self.db.execute(query)
        indices = {}
        
        for row in result:
            table_name = row.tablename
            if table_name not in indices:
                indices[table_name] = []
            indices[table_name].append({
                'name': row.indexname,
                'definition': row.indexdef
            })
        
        for table, table_indices in indices.items():
            print(f"   {table}: {len(table_indices)} indices")
            for idx in table_indices:
                print(f"     - {idx['name']}")
        
        return indices
    
    def get_recommended_indices(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get recommended indices for optimal performance"""
        
        recommendations = {
            'rvu_items': [
                {
                    'name': 'idx_rvu_hcpcs_status_effective',
                    'columns': ['hcpcs_code', 'status_code', 'effective_start'],
                    'description': 'Composite index for HCPCS lookups with status filtering'
                },
                {
                    'name': 'idx_rvu_effective_date_range',
                    'columns': ['effective_start', 'effective_end'],
                    'description': 'Range queries on effective dates'
                },
                {
                    'name': 'idx_rvu_work_rvu_not_null',
                    'columns': ['work_rvu'],
                    'condition': 'work_rvu IS NOT NULL',
                    'description': 'Partial index for non-null work RVU values'
                },
                {
                    'name': 'idx_rvu_release_effective',
                    'columns': ['release_id', 'effective_start'],
                    'description': 'Release-based queries with date filtering'
                }
            ],
            'gpci_indices': [
                {
                    'name': 'idx_gpci_mac_locality_effective',
                    'columns': ['mac', 'locality_id', 'effective_start'],
                    'description': 'MAC and locality lookups with date filtering'
                },
                {
                    'name': 'idx_gpci_state_locality_effective',
                    'columns': ['state', 'locality_id', 'effective_start'],
                    'description': 'State-based locality queries'
                },
                {
                    'name': 'idx_gpci_work_gpci_range',
                    'columns': ['work_gpci'],
                    'description': 'Range queries on work GPCI values'
                }
            ],
            'opps_caps': [
                {
                    'name': 'idx_opps_hcpcs_mac_locality_effective',
                    'columns': ['hcpcs_code', 'mac', 'locality_id', 'effective_start'],
                    'description': 'HCPCS and MAC locality lookups'
                },
                {
                    'name': 'idx_opps_price_range',
                    'columns': ['price_fac', 'price_nonfac'],
                    'description': 'Price range queries'
                }
            ],
            'anes_cfs': [
                {
                    'name': 'idx_anes_mac_locality_effective',
                    'columns': ['mac', 'locality_id', 'effective_start'],
                    'description': 'Anesthesia CF lookups by MAC and locality'
                }
            ],
            'locality_counties': [
                {
                    'name': 'idx_locco_mac_state_locality',
                    'columns': ['mac', 'state', 'locality_id'],
                    'description': 'Locality to county crosswalk queries'
                }
            ],
            'rvu_releases': [
                {
                    'name': 'idx_release_type_version_imported',
                    'columns': ['type', 'source_version', 'imported_at'],
                    'description': 'Release lookups by type and version'
                },
                {
                    'name': 'idx_release_published',
                    'columns': ['published_at'],
                    'description': 'Published release queries'
                }
            ]
        }
        
        return recommendations
    
    def create_recommended_indices(self) -> Dict[str, List[str]]:
        """Create recommended indices for performance optimization"""
        
        print("🚀 Creating recommended indices...")
        
        recommendations = self.get_recommended_indices()
        created_indices = {}
        
        for table_name, indices in recommendations.items():
            created_indices[table_name] = []
            
            for idx_config in indices:
                try:
                    # Check if index already exists
                    if self._index_exists(table_name, idx_config['name']):
                        print(f"   ⚠️  Index {idx_config['name']} already exists, skipping")
                        continue
                    
                    # Create the index
                    self._create_index(table_name, idx_config)
                    created_indices[table_name].append(idx_config['name'])
                    print(f"   ✅ Created index {idx_config['name']}")
                    
                except Exception as e:
                    logger.error(f"Failed to create index {idx_config['name']}: {e}")
                    print(f"   ❌ Failed to create index {idx_config['name']}: {e}")
        
        return created_indices
    
    def _index_exists(self, table_name: str, index_name: str) -> bool:
        """Check if an index already exists"""
        
        query = text("""
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND tablename = :table_name 
            AND indexname = :index_name
        """)
        
        result = self.db.execute(query, {
            'table_name': table_name,
            'index_name': index_name
        }).scalar()
        
        return result > 0
    
    def _create_index(self, table_name: str, idx_config: Dict[str, Any]):
        """Create a database index"""
        
        columns = ', '.join(idx_config['columns'])
        condition = idx_config.get('condition', '')
        
        if condition:
            # Partial index
            sql = f"""
                CREATE INDEX {idx_config['name']} 
                ON {table_name} ({columns}) 
                WHERE {condition}
            """
        else:
            # Full index
            sql = f"""
                CREATE INDEX {idx_config['name']} 
                ON {table_name} ({columns})
            """
        
        self.db.execute(text(sql))
        self.db.commit()
    
    def analyze_query_performance(self) -> Dict[str, Any]:
        """Analyze query performance using EXPLAIN ANALYZE"""
        
        print("📊 Analyzing query performance...")
        
        # Common query patterns to analyze
        test_queries = [
            {
                'name': 'hcpcs_lookup',
                'sql': """
                    SELECT * FROM rvu_items 
                    WHERE hcpcs_code = '99213' 
                    AND effective_start <= CURRENT_DATE 
                    AND effective_end >= CURRENT_DATE
                    LIMIT 1
                """
            },
            {
                'name': 'status_code_filter',
                'sql': """
                    SELECT * FROM rvu_items 
                    WHERE status_code IN ('A', 'R', 'T') 
                    AND effective_start <= CURRENT_DATE 
                    AND effective_end >= CURRENT_DATE
                    LIMIT 100
                """
            },
            {
                'name': 'effective_date_range',
                'sql': """
                    SELECT * FROM rvu_items 
                    WHERE effective_start >= CURRENT_DATE - INTERVAL '30 days'
                    AND effective_end <= CURRENT_DATE
                    LIMIT 1000
                """
            },
            {
                'name': 'gpci_lookup',
                'sql': """
                    SELECT * FROM gpci_indices 
                    WHERE mac = '10112' 
                    AND locality_id = '00'
                    AND effective_start <= CURRENT_DATE 
                    AND effective_end >= CURRENT_DATE
                    LIMIT 1
                """
            },
            {
                'name': 'complex_join',
                'sql': """
                    SELECT r.hcpcs_code, r.work_rvu, g.work_gpci 
                    FROM rvu_items r
                    JOIN gpci_indices g ON r.release_id = g.release_id
                    WHERE r.status_code = 'A'
                    AND r.effective_start <= CURRENT_DATE 
                    AND r.effective_end >= CURRENT_DATE
                    LIMIT 100
                """
            }
        ]
        
        results = {}
        
        for query in test_queries:
            try:
                # Get query plan
                explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query['sql']}"
                result = self.db.execute(text(explain_sql))
                plan = result.fetchone()[0][0]
                
                results[query['name']] = {
                    'execution_time': plan['Execution Time'],
                    'planning_time': plan['Planning Time'],
                    'total_time': plan['Execution Time'] + plan['Planning Time'],
                    'rows_returned': plan['Plan']['Actual Rows'],
                    'cost': plan['Plan']['Total Cost']
                }
                
                print(f"   {query['name']}: {results[query['name']]['total_time']:.2f}ms")
                
            except Exception as e:
                logger.error(f"Failed to analyze query {query['name']}: {e}")
                results[query['name']] = {'error': str(e)}
        
        return results
    
    def optimize_database(self) -> Dict[str, Any]:
        """Run complete database optimization"""
        
        print("🔧 Running database optimization...")
        print("=" * 50)
        
        # Analyze existing indices
        existing_indices = self.analyze_existing_indices()
        
        # Create recommended indices
        created_indices = self.create_recommended_indices()
        
        # Analyze query performance
        query_performance = self.analyze_query_performance()
        
        # Summary
        total_created = sum(len(indices) for indices in created_indices.values())
        
        print(f"\n📊 Optimization Summary:")
        print(f"   Existing indices: {sum(len(indices) for indices in existing_indices.values())}")
        print(f"   New indices created: {total_created}")
        
        # Check if queries meet performance targets
        slow_queries = []
        for query_name, perf in query_performance.items():
            if 'error' not in perf and perf['total_time'] > 500:  # 500ms threshold
                slow_queries.append(query_name)
        
        if slow_queries:
            print(f"   ⚠️  Slow queries: {', '.join(slow_queries)}")
        else:
            print(f"   ✅ All queries under 500ms threshold")
        
        return {
            'existing_indices': existing_indices,
            'created_indices': created_indices,
            'query_performance': query_performance,
            'total_indices_created': total_created,
            'slow_queries': slow_queries
        }
    
    def close(self):
        """Clean up resources"""
        self.db.close()




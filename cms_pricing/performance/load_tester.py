"""
Load testing framework for RVU data ingestion and queries

Tests system performance with realistic data volumes (10+ years)
"""

import time
import statistics
from typing import Dict, List, Any, Tuple
from datetime import datetime, date, timedelta
import uuid
import random
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
from cms_pricing.ingestion.rvu import RVUIngester
import logging

logger = logging.getLogger(__name__)


class LoadTester:
    """Load testing framework for RVU system performance"""
    
    def __init__(self):
        self.db = SessionLocal()
        self.ingester = RVUIngester()
    
    def generate_test_data(self, years: int = 10) -> Dict[str, Any]:
        """Generate realistic test data for multiple years"""
        
        print(f"📊 Generating {years} years of test data...")
        
        # Generate releases for each year
        releases = []
        start_date = date(2015, 1, 1)
        
        for year in range(years):
            release_date = start_date + timedelta(days=365 * year)
            
            # Create quarterly releases (Q1, Q2, Q3, Q4)
            for quarter in range(4):
                quarter_start = release_date + timedelta(days=90 * quarter)
                quarter_end = quarter_start + timedelta(days=89)
                
                release = Release(
                    id=uuid.uuid4(),
                    type="RVU_FULL",
                    source_version=f"{2015 + year}Q{quarter + 1}",
                    imported_at=quarter_start,
                    published_at=quarter_start + timedelta(days=1),
                    notes=f"Load test data for {2015 + year} Q{quarter + 1}"
                )
                releases.append(release)
        
        # Generate RVU items (realistic volume: ~20K per quarter)
        rvu_items = []
        hcpcs_codes = self._generate_hcpcs_codes(20000)  # 20K unique codes
        
        for release in releases:
            # Generate 20K RVU items per release
            for i in range(20000):
                rvu_item = RVUItem(
                    release_id=release.id,
                    hcpcs_code=random.choice(hcpcs_codes),
                    modifiers=self._generate_modifiers(),
                    modifier_key=self._generate_modifier_key(),
                    description=f"Test procedure {i}",
                    status_code=random.choice(['A', 'R', 'T', 'I', 'N']),
                    work_rvu=round(random.uniform(0, 10), 3) if random.random() > 0.1 else None,
                    pe_rvu_nonfac=round(random.uniform(0, 5), 3) if random.random() > 0.1 else None,
                    pe_rvu_fac=round(random.uniform(0, 3), 3) if random.random() > 0.1 else None,
                    mp_rvu=round(random.uniform(0, 2), 3) if random.random() > 0.1 else None,
                    na_indicator=random.choice(['0', '1']),
                    global_days=random.choice(['000', '010', '090', 'XXX']),
                    bilateral_ind=random.choice(['0', '1']),
                    multiple_proc_ind=random.choice(['0', '1']),
                    assistant_surg_ind=random.choice(['0', '1']),
                    co_surg_ind=random.choice(['0', '1']),
                    team_surg_ind=random.choice(['0', '1']),
                    endoscopic_base=random.choice(['0', '1']),
                    conversion_factor=round(random.uniform(30, 40), 4),
                    physician_supervision=random.choice(['01', '02', '03', '04', '05']),
                    diag_imaging_family=random.choice(['00', '01', '02']),
                    total_nonfac=round(random.uniform(0, 1000), 2),
                    total_fac=round(random.uniform(0, 800), 2),
                    effective_start=release.imported_at,
                    effective_end=release.imported_at + timedelta(days=89),
                    source_file=f"test_data_{release.source_version}.txt",
                    row_num=i + 1
                )
                rvu_items.append(rvu_item)
        
        # Generate GPCI indices (realistic volume: ~200 per quarter)
        gpci_indices = []
        macs = ['10112', '02102', '03102', '07102', '01112', '12102', '13102', '14102']
        states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA']
        
        for release in releases:
            for mac in macs:
                for state in states:
                    for locality in range(10):  # 10 localities per state
                        gpci = GPCIIndex(
                            release_id=release.id,
                            mac=mac,
                            state=state,
                            locality_id=f"{locality:02d}",
                            locality_name=f"Test Locality {locality}",
                            work_gpci=round(random.uniform(0.8, 1.5), 3),
                            pe_gpci=round(random.uniform(0.7, 1.3), 3),
                            mp_gpci=round(random.uniform(0.5, 1.2), 3),
                            effective_start=release.imported_at,
                            effective_end=release.imported_at + timedelta(days=89),
                            source_file=f"test_gpci_{release.source_version}.txt",
                            row_num=len(gpci_indices) + 1
                        )
                        gpci_indices.append(gpci)
        
        print(f"✅ Generated test data:")
        print(f"   - {len(releases)} releases")
        print(f"   - {len(rvu_items)} RVU items")
        print(f"   - {len(gpci_indices)} GPCI indices")
        
        return {
            'releases': releases,
            'rvu_items': rvu_items,
            'gpci_indices': gpci_indices
        }
    
    def _generate_hcpcs_codes(self, count: int) -> List[str]:
        """Generate realistic HCPCS codes"""
        codes = []
        
        # Generate common code patterns
        for i in range(count):
            if i < count // 2:
                # 5-digit numeric codes (most common)
                codes.append(f"{random.randint(10000, 99999)}")
            elif i < count * 3 // 4:
                # 5-character codes with letters
                prefix = random.choice(['A', 'B', 'C', 'D', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'P', 'Q', 'R', 'S', 'T', 'V', 'W'])
                suffix = f"{random.randint(1000, 9999)}"
                codes.append(f"{prefix}{suffix}")
            else:
                # Special codes
                codes.append(f"{random.randint(1000, 9999)}{random.choice(['F', 'T', 'U'])}")
        
        return codes
    
    def _generate_modifiers(self) -> List[str]:
        """Generate realistic modifiers"""
        if random.random() < 0.7:  # 70% have no modifiers
            return []
        
        modifiers = []
        modifier_codes = ['25', '26', '50', '51', '52', '53', '54', '55', '59', '62', '63', '66', '76', '77', '78', '79', '80', '81', '82', 'AS', 'LT', 'RT', 'TC']
        
        num_modifiers = random.randint(1, 3)
        for _ in range(num_modifiers):
            modifiers.append(random.choice(modifier_codes))
        
        return modifiers
    
    def _generate_modifier_key(self) -> str:
        """Generate modifier key"""
        modifiers = self._generate_modifiers()
        if not modifiers:
            return "null"
        return ",".join(sorted(modifiers))
    
    def load_test_data(self, years: int = 10) -> Dict[str, Any]:
        """Load test data into database and measure performance"""
        
        print(f"🚀 Starting load test with {years} years of data...")
        
        # Generate test data
        test_data = self.generate_test_data(years)
        
        # Clear existing data
        self.db.query(RVUItem).delete()
        self.db.query(GPCIIndex).delete()
        self.db.query(Release).delete()
        self.db.commit()
        
        # Load releases
        start_time = time.time()
        for release in test_data['releases']:
            self.db.add(release)
        self.db.commit()
        release_time = time.time() - start_time
        
        # Load RVU items in batches
        start_time = time.time()
        batch_size = 1000
        for i in range(0, len(test_data['rvu_items']), batch_size):
            batch = test_data['rvu_items'][i:i + batch_size]
            for item in batch:
                self.db.add(item)
            if i % 10000 == 0:
                print(f"   Loaded {i} RVU items...")
        self.db.commit()
        rvu_time = time.time() - start_time
        
        # Load GPCI indices
        start_time = time.time()
        for gpci in test_data['gpci_indices']:
            self.db.add(gpci)
        self.db.commit()
        gpci_time = time.time() - start_time
        
        total_time = release_time + rvu_time + gpci_time
        
        print(f"✅ Load test completed:")
        print(f"   - Releases: {len(test_data['releases'])} in {release_time:.2f}s")
        print(f"   - RVU Items: {len(test_data['rvu_items'])} in {rvu_time:.2f}s")
        print(f"   - GPCI Indices: {len(test_data['gpci_indices'])} in {gpci_time:.2f}s")
        print(f"   - Total time: {total_time:.2f}s")
        
        return {
            'releases_loaded': len(test_data['releases']),
            'rvu_items_loaded': len(test_data['rvu_items']),
            'gpci_indices_loaded': len(test_data['gpci_indices']),
            'release_time': release_time,
            'rvu_time': rvu_time,
            'gpci_time': gpci_time,
            'total_time': total_time
        }
    
    def query_performance_test(self, iterations: int = 100) -> Dict[str, Any]:
        """Test query performance with realistic queries"""
        
        print(f"🔍 Testing query performance with {iterations} iterations...")
        
        # Common query patterns
        queries = [
            self._test_hcpcs_lookup,
            self._test_status_code_filter,
            self._test_effective_date_range,
            self._test_gpci_lookup,
            self._test_complex_join
        ]
        
        results = {}
        
        for query_func in queries:
            query_name = query_func.__name__.replace('_test_', '')
            times = []
            
            for i in range(iterations):
                start_time = time.time()
                try:
                    query_func()
                    end_time = time.time()
                    times.append(end_time - start_time)
                except Exception as e:
                    logger.error(f"Query {query_name} failed: {e}")
                    times.append(1.0)  # Penalty for failed queries
            
            # Calculate statistics
            results[query_name] = {
                'mean': statistics.mean(times),
                'median': statistics.median(times),
                'p95': self._percentile(times, 95),
                'p99': self._percentile(times, 99),
                'min': min(times),
                'max': max(times),
                'success_rate': len([t for t in times if t < 1.0]) / len(times)
            }
            
            print(f"   {query_name}: P95={results[query_name]['p95']:.3f}s, Mean={results[query_name]['mean']:.3f}s")
        
        return results
    
    def _test_hcpcs_lookup(self):
        """Test HCPCS code lookup performance"""
        hcpcs_code = f"{random.randint(10000, 99999)}"
        self.db.query(RVUItem).filter(
            RVUItem.hcpcs_code == hcpcs_code,
            RVUItem.effective_start <= date.today(),
            RVUItem.effective_end >= date.today()
        ).first()
    
    def _test_status_code_filter(self):
        """Test status code filtering performance"""
        self.db.query(RVUItem).filter(
            RVUItem.status_code.in_(['A', 'R', 'T']),
            RVUItem.effective_start <= date.today(),
            RVUItem.effective_end >= date.today()
        ).limit(100).all()
    
    def _test_effective_date_range(self):
        """Test effective date range queries"""
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()
        
        self.db.query(RVUItem).filter(
            RVUItem.effective_start >= start_date,
            RVUItem.effective_end <= end_date
        ).limit(1000).all()
    
    def _test_gpci_lookup(self):
        """Test GPCI lookup performance"""
        mac = random.choice(['10112', '02102', '03102', '07102', '01112'])
        locality = f"{random.randint(0, 9):02d}"
        
        self.db.query(GPCIIndex).filter(
            GPCIIndex.mac == mac,
            GPCIIndex.locality_id == locality,
            GPCIIndex.effective_start <= date.today(),
            GPCIIndex.effective_end >= date.today()
        ).first()
    
    def _test_complex_join(self):
        """Test complex join queries"""
        # Join RVU items with GPCI data
        self.db.query(RVUItem).join(
            GPCIIndex, 
            RVUItem.release_id == GPCIIndex.release_id
        ).filter(
            RVUItem.status_code == 'A',
            RVUItem.effective_start <= date.today(),
            RVUItem.effective_end >= date.today()
        ).limit(100).all()
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile of data"""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]
    
    def run_full_performance_test(self, years: int = 10, query_iterations: int = 100) -> Dict[str, Any]:
        """Run complete performance test suite"""
        
        print(f"🧪 Running full performance test suite...")
        print(f"   Data volume: {years} years")
        print(f"   Query iterations: {query_iterations}")
        print("=" * 60)
        
        # Load test data
        load_results = self.load_test_data(years)
        
        # Query performance test
        query_results = self.query_performance_test(query_iterations)
        
        # Check if we meet SLOs
        p95_times = [result['p95'] for result in query_results.values()]
        max_p95 = max(p95_times) if p95_times else 0
        slo_met = max_p95 <= 0.5  # 500ms SLO
        
        print(f"\n📊 Performance Summary:")
        print(f"   Max P95 query time: {max_p95:.3f}s")
        print(f"   SLO (500ms): {'✅ PASS' if slo_met else '❌ FAIL'}")
        
        return {
            'load_results': load_results,
            'query_results': query_results,
            'slo_met': slo_met,
            'max_p95_time': max_p95
        }
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.ingester.close()




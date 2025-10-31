"""Tests for provenance columns in fee schedule models (Phase 2.2)

Validates that:
1. All fee schedule models have release_id and batch_id columns
2. Indexes are defined correctly (matching migration naming)
3. No index=True flags remain (prevents duplicate indexes)
4. Models can be instantiated with provenance fields
"""

import pytest
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from cms_pricing.models.fee_schedules import (
    FeeMPFS,
    FeeOPPS,
    FeeASC,
    FeeIPPS,
    FeeCLFS,
    FeeDMEPOS,
    GPCI,
    ConversionFactor,
    WageIndex,
    IPPSBaseRate,
)


# All fee schedule models that should have provenance columns
FEE_SCHEDULE_MODELS = [
    FeeMPFS,
    FeeOPPS,
    FeeASC,
    FeeIPPS,
    FeeCLFS,
    FeeDMEPOS,
    GPCI,
    ConversionFactor,
    WageIndex,
    IPPSBaseRate,
]

# Expected table name -> index name pattern mapping
EXPECTED_INDEXES = {
    "fee_mpfs": ("idx_fee_mpfs_release", "idx_fee_mpfs_batch"),
    "fee_opps": ("idx_fee_opps_release", "idx_fee_opps_batch"),
    "fee_asc": ("idx_fee_asc_release", "idx_fee_asc_batch"),
    "fee_ipps": ("idx_fee_ipps_release", "idx_fee_ipps_batch"),
    "fee_clfs": ("idx_fee_clfs_release", "idx_fee_clfs_batch"),
    "fee_dmepos": ("idx_fee_dmepos_release", "idx_fee_dmepos_batch"),
    "gpci": ("idx_gpci_release", "idx_gpci_batch"),
    "conversion_factors": ("idx_conversion_factors_release", "idx_conversion_factors_batch"),
    "wage_index": ("idx_wage_index_release", "idx_wage_index_batch"),
    "ipps_base_rates": ("idx_ipps_base_rates_release", "idx_ipps_base_rates_batch"),
}


@pytest.mark.unit
class TestProvenanceColumns:
    """Test that all fee schedule models have provenance columns"""

    @pytest.mark.parametrize("model_class", FEE_SCHEDULE_MODELS)
    def test_has_release_id_column(self, model_class):
        """Verify release_id column exists and is nullable String(50)"""
        mapper = inspect(model_class)
        assert "release_id" in mapper.columns, f"{model_class.__name__} missing release_id column"
        
        col = mapper.columns["release_id"]
        assert col.nullable is True, f"{model_class.__name__}.release_id should be nullable"
        assert col.type.length == 50, f"{model_class.__name__}.release_id should be String(50)"

    @pytest.mark.parametrize("model_class", FEE_SCHEDULE_MODELS)
    def test_has_batch_id_column(self, model_class):
        """Verify batch_id column exists and is nullable String(50)"""
        mapper = inspect(model_class)
        assert "batch_id" in mapper.columns, f"{model_class.__name__} missing batch_id column"
        
        col = mapper.columns["batch_id"]
        assert col.nullable is True, f"{model_class.__name__}.batch_id should be nullable"
        assert col.type.length == 50, f"{model_class.__name__}.batch_id should be String(50)"

    @pytest.mark.parametrize("model_class", FEE_SCHEDULE_MODELS)
    def test_no_index_flag_on_provenance_columns(self, model_class):
        """Verify release_id and batch_id don't have index=True (use explicit Index instead)"""
        mapper = inspect(model_class)
        
        release_col = mapper.columns["release_id"]
        batch_col = mapper.columns["batch_id"]
        
        # Check that column-level index flag is False (indexes defined in __table_args__)
        assert not release_col.index, f"{model_class.__name__}.release_id should not have index=True"
        assert not batch_col.index, f"{model_class.__name__}.batch_id should not have index=True"

    @pytest.mark.parametrize("model_class", FEE_SCHEDULE_MODELS)
    def test_provenance_indexes_defined(self, model_class):
        """Verify explicit Index objects exist in __table_args__ matching migration naming"""
        table_name = model_class.__tablename__
        assert table_name in EXPECTED_INDEXES, f"Missing index mapping for {table_name}"
        
        expected_release_idx, expected_batch_idx = EXPECTED_INDEXES[table_name]
        
        # Get all index names from __table_args__
        table_args = model_class.__table_args__
        if isinstance(table_args, tuple):
            index_names = [idx.name for idx in table_args if hasattr(idx, 'name')]
        else:
            index_names = []
        
        assert expected_release_idx in index_names, (
            f"{model_class.__name__} missing index {expected_release_idx} in __table_args__"
        )
        assert expected_batch_idx in index_names, (
            f"{model_class.__name__} missing index {expected_batch_idx} in __table_args__"
        )

    @pytest.mark.parametrize("model_class", FEE_SCHEDULE_MODELS)
    def test_can_instantiate_with_provenance(self, model_class):
        """Verify models can be instantiated with release_id and batch_id"""
        # Create minimal valid instance
        instance = model_class()
        
        # Set provenance fields
        instance.release_id = "test_release_001"
        instance.batch_id = "test_batch_001"
        
        assert instance.release_id == "test_release_001"
        assert instance.batch_id == "test_batch_001"
        
        # Verify can also be None (nullable)
        instance.release_id = None
        instance.batch_id = None
        assert instance.release_id is None
        assert instance.batch_id is None


@pytest.mark.integration
class TestProvenanceMigrationCompatibility:
    """Test that models match migration DDL (requires database)"""

    def test_columns_match_migration(self, test_db_session):
        """Verify actual database columns match model definitions"""
        from sqlalchemy import inspect as sql_inspect
        
        engine = test_db_session.bind
        inspector = sql_inspect(engine)
        
        for model_class in FEE_SCHEDULE_MODELS:
            table_name = model_class.__tablename__
            
            # Check if table exists (migration may not have run yet in some test setups)
            if not inspector.has_table(table_name):
                pytest.skip(f"Table {table_name} does not exist - migration not run yet")
            
            columns = {col["name"]: col for col in inspector.get_columns(table_name)}
            
            # Verify release_id column
            assert "release_id" in columns, f"{table_name} missing release_id column in database"
            release_col = columns["release_id"]
            assert release_col["nullable"] is True, f"{table_name}.release_id should be nullable"
            assert release_col["type"].length == 50, f"{table_name}.release_id should be VARCHAR(50)"
            
            # Verify batch_id column
            assert "batch_id" in columns, f"{table_name} missing batch_id column in database"
            batch_col = columns["batch_id"]
            assert batch_col["nullable"] is True, f"{table_name}.batch_id should be nullable"
            assert batch_col["type"].length == 50, f"{table_name}.batch_id should be VARCHAR(50)"

    def test_indexes_match_migration(self, test_db_session):
        """Verify actual database indexes match model definitions"""
        from sqlalchemy import inspect as sql_inspect, text
        
        engine = test_db_session.bind
        inspector = sql_inspect(engine)
        
        for model_class in FEE_SCHEDULE_MODELS:
            table_name = model_class.__tablename__
            
            if not inspector.has_table(table_name):
                pytest.skip(f"Table {table_name} does not exist - migration not run yet")
            
            # Get all indexes for this table
            indexes = {idx["name"]: idx for idx in inspector.get_indexes(table_name)}
            
            # Check expected indexes exist
            expected_release_idx, expected_batch_idx = EXPECTED_INDEXES[table_name]
            
            assert expected_release_idx in indexes, (
                f"{table_name} missing index {expected_release_idx} in database"
            )
            assert expected_batch_idx in indexes, (
                f"{table_name} missing index {expected_batch_idx} in database"
            )
            
            # Verify index columns
            release_idx = indexes[expected_release_idx]
            batch_idx = indexes[expected_batch_idx]
            
            assert "release_id" in release_idx["column_names"], (
                f"{expected_release_idx} should index release_id"
            )
            assert "batch_id" in batch_idx["column_names"], (
                f"{expected_batch_idx} should index batch_id"
            )


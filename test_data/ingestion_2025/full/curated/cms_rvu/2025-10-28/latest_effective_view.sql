
            CREATE OR REPLACE VIEW v_latest_cms_rvu AS
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY hcpcs_code 
                           ORDER BY effective_from DESC, vintage_date DESC
                       ) as rn
                FROM cms_rvu
                WHERE effective_from <= CURRENT_DATE
            ) ranked
            WHERE rn = 1;
            
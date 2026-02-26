# Plan: Add Warehouse Configuration

## Changes

### 1. [setup.sql](revenue_operations/setup.sql) - Line 88-96
Replace the commented warehouse section with active creation:

```sql
-- ────────────────────────────────────────────────────────────
-- 5. Warehouse
-- ────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS MY_WAREHOUSE
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
GRANT USAGE ON WAREHOUSE MY_WAREHOUSE TO ROLE <YOUR_DBT_ROLE>;
```

### 2. [profiles.yml](revenue_operations/profiles.yml) - Line 9
Change:
```yaml
warehouse: WH_XS
```
To:
```yaml
warehouse: MY_WAREHOUSE
```

### 3. Push to repo
Commit message: "Add MY_WAREHOUSE creation and update dbt profile"
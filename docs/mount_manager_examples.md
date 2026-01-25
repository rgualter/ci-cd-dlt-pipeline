# ADLS Mount Manager - Usage Examples

This document provides real-world usage scenarios for the `ADLSMountManager` class.

## Prerequisites

Ensure you have configured your Key Vault scopes and secrets before attempting to mount containers. See the [main README](../README.md) for details.

---


## Basic Usage

```python
from utils.mount_manager import ADLSMountManager

# Initialize mount manager
mount_manager = ADLSMountManager(
    dbutils=dbutils,
    key_vault_scope='formula1-scope',
    client_id_key='app-client-id',
    tenant_id_key='app-tenant-id',
    client_secret_key='app-secret-id'
)

# Mount containers
mount_manager.mount_containers(
    storage_account_name='gualterformula1dl',
    container_names=['raw', 'processed', 'presentation']
)

# View mounted containers
mount_manager.list_mounts('gualterformula1dl')
```

---

## Scenario 1: Add a New Container to Existing Storage Account

You need an 'archive' container for old data:

```python
# Mount just the new container
mount_manager.mount_containers('gualterformula1dl', ['archive'])

# Or remount everything with the new container
containers = ['raw', 'processed', 'presentation', 'archive']
results = mount_manager.mount_containers('gualterformula1dl', containers)

print(f"Mounted containers: {list(results.keys())}")
# Output: Mounted containers: ['raw', 'processed', 'presentation', 'archive']
```

---

## Scenario 2: Temporary Mount for a Different Storage Account

Access data from another project temporarily without changing your main configuration:

```python
# Mount partner's storage (using same credentials from Key Vault)
temporary_results = mount_manager.mount_containers(
    storage_account_name='partnercompanydl',
    container_names=['shared-data', 'exports']
)

# Available mount points:
# /mnt/gualterformula1dl/raw          (your original mounts)
# /mnt/gualterformula1dl/processed    (your original mounts)
# /mnt/gualterformula1dl/presentation (your original mounts)
# /mnt/partnercompanydl/shared-data   (temporary mount)
# /mnt/partnercompanydl/exports       (temporary mount)

# Read data from partner
df_partner = spark.read.parquet("/mnt/partnercompanydl/shared-data/customer_data.parquet")

# When done, unmount
mount_manager.unmount_containers('partnercompanydl', ['shared-data', 'exports'])
```

---

## Scenario 3: Multi-Environment Setup (DEV/STAGING/PROD)

Different Key Vaults and storage accounts per environment:

```python
# Development Environment
mount_manager_dev = ADLSMountManager(key_vault_scope='formula1-dev-scope')
dev_results = mount_manager_dev.mount_containers(
    storage_account_name='formula1devdl',
    container_names=['raw', 'processed']  # Fewer containers in dev
)

# Production Environment
mount_manager_prod = ADLSMountManager(key_vault_scope='formula1-scope')
prod_results = mount_manager_prod.mount_containers(
    storage_account_name='gualterformula1dl',
    container_names=['raw', 'processed', 'presentation']
)

# View mounts by environment
print("=== DEV Mounts ===")
mount_manager_dev.list_mounts('formula1devdl')

print("=== PROD Mounts ===")
mount_manager_prod.list_mounts('gualterformula1dl')
```

---

## Scenario 4: Working with Multiple Projects

Mount containers from multiple projects simultaneously:

```python
# Project 1: Formula1
formula1_manager = ADLSMountManager(key_vault_scope='formula1-scope')
formula1_manager.mount_containers(
    'gualterformula1dl',
    ['raw', 'processed', 'presentation']
)

# Project 2: Sales Analytics
sales_manager = ADLSMountManager(key_vault_scope='sales-scope')
sales_manager.mount_containers(
    'salesanalysisdl',
    ['bronze', 'silver', 'gold']
)

# Project 3: Customer 360
customer_manager = ADLSMountManager(key_vault_scope='customer360-scope')
customer_manager.mount_containers(
    'customer360dl',
    ['landing', 'curated', 'analytics']
)

# All projects are now mounted simultaneously:
# /mnt/gualterformula1dl/raw
# /mnt/gualterformula1dl/processed
# /mnt/gualterformula1dl/presentation
# /mnt/salesanalysisdl/bronze
# /mnt/salesanalysisdl/silver
# /mnt/salesanalysisdl/gold
# /mnt/customer360dl/landing
# /mnt/customer360dl/curated
# /mnt/customer360dl/analytics
```

---

## Scenario 5: Dynamic Container Management

Mount containers based on a configuration table:

```python
# Read container list from a configuration table
container_config_df = spark.sql("""
    SELECT storage_account, container_name, is_active
    FROM config.mount_configuration
    WHERE is_active = true
""")

# Mount all active containers dynamically
for row in container_config_df.collect():
    mount_manager.mount_containers(
        storage_account_name=row['storage_account'],
        container_names=[row['container_name']]
    )
```

---

## Scenario 6: Selective Remounting (Refresh)

Refresh mounts when credentials are rotated or a mount is corrupted:

```python
# Remount only the 'raw' container
mount_manager.mount_containers(
    'gualterformula1dl', 
    ['raw'], 
    force_remount=True  # This will unmount and remount
)

# Or remount all containers (useful after credential rotation)
results = mount_manager.mount_containers(
    'gualterformula1dl',
    ['raw', 'processed', 'presentation'],
    force_remount=True
)
```

---

## Scenario 7: Notebook Parameter-Based Mounting

Use Databricks widgets for environment flexibility:

```python
# Create widget
dbutils.widgets.dropdown("environment", "prod", ["dev", "staging", "prod"])
environment = dbutils.widgets.get("environment")

# Environment-specific configuration
env_configs = {
    'dev': {
        'key_vault_scope': 'formula1-dev-scope',
        'storage_account': 'formula1devdl',
        'containers': ['raw', 'processed']
    },
    'staging': {
        'key_vault_scope': 'formula1-staging-scope',
        'storage_account': 'formula1stagingdl',
        'containers': ['raw', 'processed', 'presentation']
    },
    'prod': {
        'key_vault_scope': 'formula1-scope',
        'storage_account': 'gualterformula1dl',
        'containers': ['raw', 'processed', 'presentation', 'archive']
    }
}

# Mount based on selected environment
selected_config = env_configs[environment]
env_manager = ADLSMountManager(key_vault_scope=selected_config['key_vault_scope'])
env_manager.mount_containers(
    selected_config['storage_account'],
    selected_config['containers']
)

print(f"Mounted {environment.upper()} environment successfully!")
```

---

## Scenario 8: Cleanup Script (Unmount Everything)

Clean up all mounts for a storage account:

```python
# Unmount all containers for the storage account
results = mount_manager.unmount_containers(
    'gualterformula1dl',
    ['raw', 'processed', 'presentation']
)
print(f"Unmounted {sum(results.values())} containers")
```

---

## Scenario 9: Error Handling and Validation

Check which mounts succeeded and which failed:

```python
results = mount_manager.mount_containers(
    'gualterformula1dl',
    ['raw', 'processed', 'presentation', 'nonexistent']  # One will fail
)

# Analyze results
successful_mounts = [k for k, v in results.items() if v]
failed_mounts = [k for k, v in results.items() if not v]

print(f"Successful: {successful_mounts}")
print(f"Failed: {failed_mounts}")

# Take action based on failures
if failed_mounts:
    print(f"Warning: {len(failed_mounts)} containers failed to mount")
    # Send alert, log to table, etc.
else:
    print("All containers mounted successfully!")
```

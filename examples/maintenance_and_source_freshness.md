# Source freshness and maintenance tasks
## Source freshness

If you have external data uploads, you may find it useful to check the freshness of the data. 
Check for freshness behaves the same way as wait of dependency. 
If the source is not fresh, the model will not be executed.

To enable source freshness check, you need to add `freshness` section to your source's yaml file 
(see [dbt docs](https://docs.getdbt.com/reference/resource-properties/freshness)).

```yaml
sources:
  - name: my_source
    freshness:
      error_after:
        count: 15  # positive integer
        period: hour  # minute | hour | day
    loaded_at_field: my_source_loaded_at  # column or expression on which to check the freshness
```
If this section is present of any source, 
all models that depend on this source will have a sensor that checks the freshness.

## Maintenance tasks

In each warehouse, there are some tasks to optimize the storage usage used by tables. 
In this tutorial, we will cover available tasks for _Databricks_.

> :grey_exclamation: Support of other warehouses will be added in the future.

> :warning: Dbt macros will be moved to separate package in the future.

> To use the maintenance tasks in your project, you need to copy files from [macros](dags/macros) directory to your dbt project.

### Databricks maintenance tasks

#### Set TTL

To enable the automatic deletion of data, you can set the Time To Live (TTL) for a table.
The TTL is the time in days that a record is kept in the table.
After the TTL has expired, the record is deleted.

It's required to have a column with timestamp-like data type to use the TTL feature.
Usually, it's a column that specifies the time when the record was created or updated.
This column **must monotonically increase** with time.

To set the TTL for a table, use the `maintenance` section in model's config:

```yaml
config:
  maintenance:
    ttl:
      key:  # Table timestamp-like column name
      expiration_timeout:  # Table expiration timeout in days
      additional_predicate:  # Additional predicate for filtering expired data
      force_predicate:  # Force predicate for filtering expired data. It will override base predicate
```

**Examples**:

Records that are older than 10 days will be deleted:

```yaml
config:
  maintenance:
    ttl:
      key: etl_updated_dttm
      expiration_timeout: 10
```

Records that are older than 10 days and have `status = 'accepted'` will be deleted:

```yaml
config:
  maintenance:
    ttl:
      key: etl_updated_dttm
      expiration_timeout: 10
      additional_predicate: "status = 'accepted'"
```

Records that have `rejected` status will be deleted (note that `expiration_timeout` is ignored here):

```yaml
config:
  maintenance:
    ttl:
      key: etl_updated_dttm  # ignored
      expiration_timeout: 10  # ignored
      force_predicate: order_id in (select order_id from orders where status = 'rejected')
```

#### VACUUM ([docs](https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html))

```yaml
config:
  maintenance:
    vacuum_table: true
```

#### OPTIMIZE ([docs](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html))

```yaml
config:
  maintenance:
    optimize_table: true
```

#### Deduplicate Table

Deduplicates table using `unique_key` and `partition_by` columns.

> :exclamation: This feature is not well documented. Please use it with caution.

```yaml
config:
  maintenance:
    deduplicate_table: true
```

#### Persist docs

> :exclamation: This feature is not well documented. Please use it with caution.
```yaml
config:
  maintenance:
    persist_docs: true
```
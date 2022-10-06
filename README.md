# Airflow DAG invariant testing

Note: **Contributions welcome**. I hope this library will belong to the Airflow
community.

The library in this directory is a prototype library to develop tests for
Airflow. Dagcheck was created so that Airflow users could write tests for their
DAGs with these characteristics:

- They are easy to read through and understand
- They do not orchestrate real infrastructure changes
- They run on a local development environment
- They run quickly as part of a developer's flow
- They can be run in CI/CD and catch issues in the future

Dagcheck provides 

TODO(pabloem): Add more information about the library

## Configuring `dagcheck`

TODO(pabloem)

## Caveats and pitfalls

Dagcheck works by simulating DAG execution scenarios.

### DAGs that are dependent on side effects

Dagcheck simulates DAG executions, but it will not orchestrate any changes. If
parts of your DAG execution depend on side effects from other operators, then
Dagcheck **will not know** about this.

For example, consider a DAG that performs a database export operation, checks
the output of those files, and uses them for something else. Something like:

```python
(
  DatabaseExportOperator(
    'data_warehouse_export'
    destination='database_export_file',
    ...
  ) >>
  CheckFileExistsOperator(
    'check_export_went_well'
    filename='database_export_file'
  ) >>
  ArchiveFileInColdStorageOperator(
    'save_backup_to_storage'
    ...
  )
)
```

In the above code sample, the following statement is true:

- **If the database export runs properly**, then the file existence check
   **should** succeed. and the archiving operator will run.
- This is because there is an implicit assumption that if `data_warehouse_export`
    runs properly (i.e. succeeds), then `check_export_went_well` *will succeed*.

But the following dagcheck test will fail:

```python
# Bad test example:
assert_that(
  given(the_dag)
  .when('data_warehouse_export', succeeds())
  .then('save_backup_to_storage', will_run())
)
```

This test fails because Dagcheck does not know about the implicit assumption,
and assumes that the intermediate task between `data_warehouse_export` and
`save_backup_to_storage` *may still fail*.

There are a couple ways to write this test to work well with dagcheck. Here's
one of them:

```python
# Good test example:
assert_that(
  given(the_dag)
  .when('data_warehouse_export', succeeds())
  .and_('check_export_went_well', succeeds())
  .then('save_backup_to_storage', will_run())
)
```

## TODOs before first launch

- Figure out the name of the library (dagcheck? dagtest? flowtest? ilikedags? flowcheck?, assertflow?)
- Figure out whether this belongs to airflow or is an independent library
- Implement DAG-failure and DAG-assumption checkers.

### Raw Development Notes

- 2022/09/16: Picking up the development environment again

I started developing the library as part of airflow/, and later put it in the
`airflow_play/dagcheck/` directory. Because of this, a lot of import paths in the 
`dagcheck/` directory are hacked up.

Currently, dagcheck tests require an Airflow instance running. To set up the local
development environment for dagcheck, you need to run:

```shell
# From airflow_play/

# Activate your local virtualenv
. venv/bin/activate

# Run your standalone Airflow instance that runs beside the code
export AIRFLOW_HOME=~/codes/airflow_play/home/
airflow standalone
```

Once that is set up, you can run tests.

# Airflow DAG invariant testing

Note: **Contributions welcome**. I hope this library will belong to the Airflow
community.

Dagcheck is a framework to assert for DAG invariants. Users of dagcheck can
define DAG invariants to test via assertions, and dagcheck will generate DAG
run scenarios that verify these invariants.

Dagcheck was created so that Airflow users could write tests for their
DAGs with these characteristics:

- They are easy to read through and understand
- They do not orchestrate real infrastructure changes
- They run on a local development environment
- They run quickly as part of a developer's flow
- They can be run in CI/CD and catch issues in the future

`dagcheck` is especially useful for DAGs that are complex, and that change
over time. Tests from `dagcheck` allow you to offload complex dependency
checks from your head onto an automatic test.

## Examples

Consider this [example of a complex DAG](https://airflow.apache.org/docs/apache-airflow/stable/usage-cli.html#exporting-dag-structure-as-an-image).
This DAG has several possible execution paths - and in case of failures, we may
want to ensure that it will not leak resources. For example, we may write a
test that checks that if we create a resource successfully, we will always
clean it up independently of any failure scenario.

For example, if the task `create_entry_group` succeeds, then we check that
the task `delete_entry_group` will **always** run, like so:


```python
example_dag = DagBag().get_dag("example_complex")

# First check: If we create an entry group, we want to make sure
# it will be cleaned up.
assert_that(
    given(example_dag)
    .when(task("create_entry_group"), succeeds())
    .then(task("delete_entry_group"), will_run())
)
```

By creating this test, and running it in CI, we can quickly make sure that
our DAG will behave as expected, no matter how much it changes.

To see other examples of usage of the API, look at [our unit tests](
dagcheck/tests/dagcheck_simple_api_test.py) and our [small sample DAGs](
dagcheck/tests/dagcheck_test_example_dags_utils.py).

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
  .then('check_export_went_well', will_run())

assert_that(
  given(the_dag)
  .when('check_export_went_well', succeeds())
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

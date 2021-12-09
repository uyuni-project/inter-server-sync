# Testing

## Quick start

With the following command you can let all the available tests be executed.

```shell
go test ./... -cover -race
```

We keep checking for the **race conditions** by default, although there is no concurrency implemented just yet. However,
we'd better still have it, especially since the async I/O should come around in the future. This command is also
included in the github-actions workflow.

## Structure

### General

All the test files have a postfix `_test` in their names. Besides, a testing function should have a signature of a
following form `func Test<your-name>(t *testing.T) {...}`. According to the common convention we also keep the test
files close to the corresponding functionality we test. These rules allow the runtime, and also anyone who contributes
to the test suit, to pick up the tests automatically without further need to specify the paths. The test code gets
excluded from the build automatically as well.

### Code reuse

There are a couple of repeating steps we make to prepare a test base. For this reason we shifted some common code up to
the `./tests/` package. Whenever you need to mock an object external to the functionality being tested, it would be a
good idea to put it into this common package, since chances are it will be reused somewhere else in the test suit. 

## Common cases

### Testing and/or mocking the I/O operations

You can create a mocked data repository object by calling 

```go
var repo *tests.DataRepository = tests.CreateDataRepository()
```
The repository is able to simulate reading from a sql database and writing the results into a buffer. 

On the data repository we can define what sql statements we are expecting from our code. Expect adds data to repository, 
which can then be retrieved by the tested function.

```go
repo.Expect("SELECT id, fk_id FROM tableOne;", 1)
repo.Expect("SELECT id, fk_id FROM tableTwo;", 1)
```
The data repository expect these statements in the exact same order. 

**Important:** the SQL statements check is exclusive. If the code tries to submit an SQL-statement not expected by the 
repository, the test will fail. However, if there are any expectations we defined that were left unexecuted, we need to 
check for them manually. 

`ExpectationsWereMet` checks whether all queued expectations were met in order. If any of them was not met, 
an error is returned.

```go
// checks if all expected statements were indeed executed against the db
if err := testCase.repo.ExpectationsWereMet(); err != nil {
    t.Errorf("Some nodes left unexported. Error message: %s", err)
}
```

When testing the writing behavior, you can get the contents of the buffer by calling 
```go
var writtenBuffer []string = testCase.repo.GetWriterBuffer()
```





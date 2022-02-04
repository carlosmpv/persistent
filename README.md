# Persistent

A simple tool for storing structs in key value based storages.

**It does not provide the database.**



### How to use it

 1) Add it to your project
    ```sh
    go get github.com/carlosmpv/persistent
    ```

 2) Then, implement the `Storager` interface.
    ```go
    type exampleStorager struct {
        db map[string][]byte
    }

    // stores a value (v) with a key (k)
    func (ts *exampleStorager) Save(k, v []byte) error {
        ts.db[string(k)] = v
        return nil
    }

    // gets the value given a key (k)
    func (ts *exampleStorager) Load(k []byte) ([]byte, error) {
        val, ok := ts.db[string(k)]
        if !ok {
            return nil, errors.New("value is not stored")
        }

        return val, nil
    }
    ```

    This is a very simple storager that uses a `map[string][]byte` as database. It could be a redis `set` and `get` instead on each method.


### Persistent variables

Persistent variables are wrappers around golang's standard variable types that uses a given storager to save and load its values.


**Declaring a persistent variable**

```go
str := NewPersistentString("Hello world!")
empty := EmptyPersistentString()
```
Those are pointers, so to get its value:

```go
... := *str
```

**Persisting a value**
```go
err := str.Persist(storager, "key")
if err != nil {
    ...
}
```

**Restoring a value**
```go
err := nStr.Restore(storager, "key")
// *nStr == *str
```

Structs that contains persistent fields can also be persisted and restored using `PersistStruct` and `RestoreStruct`

Check `persistent_test.go` and its test cases to better understand how to use it

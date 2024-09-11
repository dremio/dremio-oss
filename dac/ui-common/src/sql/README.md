# Modules that parse queries using ANTLR

## Note on test failures

If there are test cases that cause parsing errors, Jest may fail with an unhelpful error in the following format:

```
TypeError: Converting circular structure to JSON
    --> starting at object with constructor 'Object'
    |     property 'source' -> object with constructor 'DremioLexer'
    --- property '_tokenFactorySourcePair' closes the circle
    at stringify (<anonymous>)
```

This is because Jest internally tries to serialize exceptions generated while running the test, using JSON.stringify,
and ANTLR ParseTree objects are doubly-linked, i.e. the parent node has child pointers and the children have parent
pointers.

To see the actual exception to diagnose your test failures, you can use Jest's runInBand flag to prevent this
serialization needed for IPC, e.g.:
node node_modules/jest/bin/jest.js --runInBand

See https:github.com/jestjs/jest/issues/10577

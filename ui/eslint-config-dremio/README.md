# dremio-eslint-config

This package contains a shared ESLint configuration that can be used in any Dremio JS/TS project.

## Usage

In your project’s eslint configuration, you can extend any of the available rulesets in this package:

```json
{
  "eslintConfig": {
    "extends": ["dremio", "dremio/typescript", "dremio/react", "dremio/mocha"]
  }
}
```

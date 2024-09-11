# OSS License Checker

This module verifies that a given pnpm-lock.yaml does not contain any packages (including transitive dependencies) with disallowed licenses for the Dremio OSS release.

## Usage

```bash
pnpm licenses list --json --prod | dremio-oss-license-checker
```

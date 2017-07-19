Dremio Pseudo-Distributed FileSystem Plugin
===========================================

Description
-----------
This storage plugin allows to query local filesystem metadata on remote
nodes instances. However, it only allows to open and/or create file
on the local filesystem.

The filesystem is organized with the following hierarchy:

```
/
├── <dir1>
│   ├── <address1>@<file1>
│   ├── <address2>@<file1>
│   └── <address1>@<file2>
├── <dir2>
```

where <address?> are the node addresses

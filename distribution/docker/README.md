# Dremio Docker Image
## Image Build

You can build this image by identifying a Dremio download tarball download URL and then running the following command:

``` bash
docker build --build-arg DOWNLOAD_URL=<URL> -t "dremio-oss:2.0.5" .
```

Note: This should work for both Community and Enterprise editions of Dremio.

---

## Single Node Deployment

```bash
docker run -p 9047:9047 -p 31010:31010 -p 32010:32010 -p 45678:45678 dremio/dremio-oss
```
This includes a single node deployment that starts up a single daemon that includes:
* Embedded Zookeeper
* Master Coordinator
* Executor

---

## Multi-node Deployment

Use containers in a Kubernetes environment to deploy multi-node Dremio. See the published [helm chart](https://github.com/dremio/dremio-cloud-tools/tree/master/charts/dremio) for instructions.

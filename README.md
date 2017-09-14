# Dremio

Dremio enables organizations to unlock the value of their data.

## Documentation

Documentation is available at https://docs.dremio.com. 

## Quickstart: How to build and run Dremio

### (a) Prerequisites

* Oracle JDK 7 (some tests currently fail on JDK 8)
* Protobuf 2.5 (using Homebrew: `brew install protobuf250 && brew link protobuf250 --force`)
* (Optional) Maven 3.3.9 or later (using Homebrew: `brew install maven`)

Run the following commands to verify that you have the correct versions of Maven and JDK installed:

    java -version
    protoc --version
    mvn --version

### (b) Clone the Repository
    git clone https://github.com/dremio/dremio-oss.git dremio

### (c) Build the Code

    cd dremio
    mvn clean install -DskipTests (or ./mvnw clean install -DskipTests if maven is not installed on the machine)

The "-DskipTests" option skips most of the tests. Running all tests takes a long time.

### (d) Run/Install

#### Run

    distribution/server/target/dremio-community-{DREMIO_VERSION}/dremio-community-{DREMIO_VERSION}/bin/dremio start

OR to start a server with a default user (dremio/dremio123)

    mvn compile exec:exec -pl dac/daemon

Once run, the UI is accessible at:

    http://localhost:9047

#### Production Install

##### (1) Unpack the tarball to install.

    mkdir /opt/dremio
    tar xvzf distribution/server/target/*.tar.gz --strip=1 -C /opt/dremio

##### (2) Start Dremio Embedded Mode

    cd /opt/dremio
    bin/dremio

#### OSS Only

To have the best possible experience with Dremio, we include a number of dependencies when building Dremio that are distributed under non-oss free (as in beer) licenses. 
Examples include drivers for major databases such as Oracle Database, Microsoft SQL Server, MySQL as well as enhancements to improve source pushdowns and thread 
scheduling. If you'd like to only include dependencies with OSS licenses, Dremio will continue to work but some features will be unavailable (such as 
connecting to databases that rely on these drivers). 

To build dremio with only OSS dependencies, you can add the following option to your Maven commandline: `-Ddremio.oss-only=true`

The distribution directory will be `distribution/server/target/dremio-oss-{DREMIO_VERSION}/dremio-oss-{DREMIO_VERSION}`

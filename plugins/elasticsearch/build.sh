#!/bin/bash

../../distribution/server/target/dremio-community-1.1.0-201708121825170680-436784e/dremio-community-1.1.0-201708121825170680-436784e/bin/dremio stop

mvn package -DskipTests

cp target/dremio-elasticsearch-plugin-1.1.0-201708121825170680-436784e.jar ../../distribution/server/target/dremio-community-1.1.0-201708121825170680-436784e/dremio-community-1.1.0-201708121825170680-436784e/jars

../../distribution/server/target/dremio-community-1.1.0-201708121825170680-436784e/dremio-community-1.1.0-201708121825170680-436784e/bin/dremio start


# Dremio DAC Daemon

The single container for all of the services in Dremio.

This daemon currently this manages the Dremio Web UI,
launching an embedded Zookeeper instance and launching
a SabotNode.

To run the Daemon:
`mvn exec:exec`

There is an issue serving the Javascript files for the UI
from the java server. For now to actually use the site
the JS files must be served from the development Node server.
This is being addressed in DX-311.

To run the node server go to dac/ui, instructions for building
and running the server are there in the README. Both the daemon
and the Node server must be run to populate the UI with the test
data.

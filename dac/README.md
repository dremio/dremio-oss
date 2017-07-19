# Dremio Analyst Center

Dremio Analyst Center is the management component of Dremio. This includes both the Java based server and React based client side portions of this system.

The web server can no longer be used without the query engine also running, therefore it cannot be launched from this module. Please see the README
in the daemon module for instructions on running the Dremio daemon, which will start the query engine, the webserver and any other required services.

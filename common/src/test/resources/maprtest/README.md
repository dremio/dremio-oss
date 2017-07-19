# MapR based Unit Tests 

These are additional configuraiton files for running unit tests using the MapR build

<em> MapR based Dremio build is produced while using mapr profile </em>

#### <em> testconfig </em> directory 

testconfig directory contains MapR configuration files

#### Command to run Unit Tests for MapR build

To run the MapR build, you must also pass the root path 
of the src tree as a system property.  Example command:

<code> mvn test -Pmapr -DWORKSPACE=\<ROOT SRC PATH\> </code>
 
 


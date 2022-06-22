# Dremio Analyst Center technical doc.

to run the tests:
mvn test -pl dac/backend

see dac/backend/target/test-access_logs for REST interactions in the unit tests

# stack

We use:
 - Jetty as a web server
 - Jersey for binding URLs and requests to their implementations
 - Jackson for JSON marshalling/unmarshalling
 - protostuff for the persistence layer and also JSON binding in many cases.
 - MapDB for persistence layer

# Explore

## Dataset creation, transformation and save.

Virtual datasets are identified by their path:
space.folder1.folder2.dataset_name

The current version of a dataset is identified by a version number.

To create a dataset we initially need to provide either a dataset (select * from dataset) or a SQL query.
/dataset/{name}/new?parentDataset={name}
/dataset/{name}/new_sql?sql={sql}

From there we can modify a dataset by applying a transformation.

/dataset/{name}/version/{version}

All the transformations are defined in the protobuf file: Transform*
dac/backend/src/main/proto/dataset.proto 

A transform is applied to a given dataset version and produces a new version. This is also how the history can be navigated.

Then to save the dataset we provide the version which becoming the new head for that dataset name.

## Inheritance with proto generated files.

To unify polymorphism in protostuf and in jackson we use the following pattern:

- in protostuff we need a wrapper message to act as a union between all the possible subtypes. (ex: Transform).
Thy message has a type field. This field is an enum with one value for each possible subclass. (ex: TransformType)

- for Jackson we define a Base Type (example: TransformBase) to be extended by all the subclasses. 
- configure protostuff (dac/backend/pom.xml) so that it adds the extends statement when it generate the classes:
                <property>
                  <name>MyClassName.extends_declaration</name>
                  <value>extends com.dremio.dac.MyBaseClass</value>
                </property>

- see com.dremio.dac.modelv2.dataset.TransformBase as a base class example to define a Visitor and Jackson mappings

## Adding a transform:
 - add a Transform* message in dataset.proto and a corresponding TransformType enum value.
 - add the base class extends in dac/backend/pom.xml
 - add the visit method in TransformBase.TransformVisitor

For transforms that follow the pattern of applying to a given field (with the standard newField and dropOldField checkbox) you use FieldTransform and add FieldTransforms instead of a top level transform.





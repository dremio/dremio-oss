# TOC
 - [Resources](#v2-resources)
 - [JobData](#v2-data)
 - [Others](#v2-others)

#V2 Resources: 
## Resource defined by class com.dremio.dac.server.TimingApplicationEventListener


## Resource defined by class com.dremio.provision.resource.ProvisioningResource

 - POST /provision/cluster   
   > `=>` [com.dremio.provision.ClusterCreateRequest](#class-comdremioprovisionclustercreaterequest)   
   > `<=` [com.dremio.provision.ClusterResponse](#class-comdremioprovisionclusterresponse)   

 - DELETE /provision/cluster/{id} (path params: id={String})   
   > `<=` void   

 - GET /provision/cluster/{id} (path params: id={String})   
   > `<=` [com.dremio.provision.ClusterResponse](#class-comdremioprovisionclusterresponse)   

 - PUT /provision/cluster/{id} (path params: id={String})   
   > `=>` [com.dremio.provision.ClusterModifyRequest](#class-comdremioprovisionclustermodifyrequest)   
   > `<=` [com.dremio.provision.ClusterResponse](#class-comdremioprovisionclusterresponse)   

 - PUT /provision/cluster/{id}/dynamicConfig (path params: id={String})   
   > `=>` [com.dremio.provision.ResizeClusterRequest](#class-comdremioprovisionresizeclusterrequest)   
   > `<=` [com.dremio.provision.ClusterResponse](#class-comdremioprovisionclusterresponse)   

 - GET /provision/clusters?type={String}   
   > `<=` [com.dremio.provision.ClusterResponses](#class-comdremioprovisionclusterresponses)   



#V2 JobData
## `class com.dremio.provision.ClusterCreateRequest`
- Example:
```
{
  clusterType: "YARN" | "MESOS" | "AWS" | "KUBERNETES" | "GCE" | "AZURE",
  distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
  dynamicConfig: {
    containerCount: 1,
  },
  isSecure: true | false,
  memoryMB: 1,
  name: "abc",
  queue: "abc",
  subPropertyList: [
    {
      key: "abc",
      value: "abc",
    },
    ...
  ],
  virtualCoreCount: 1,
}
```

## `class com.dremio.provision.ClusterModifyRequest`
- Example:
```
{
  clusterType: "YARN" | "MESOS" | "AWS" | "KUBERNETES" | "GCE" | "AZURE",
  desiredState: "DELETED" | "RUNNING" | "STOPPED",
  distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
  dynamicConfig: {
    containerCount: 1,
  },
  id: "abc",
  isSecure: true | false,
  memoryMB: 1,
  name: "abc",
  queue: "abc",
  subPropertyList: [
    {
      key: "abc",
      value: "abc",
    },
    ...
  ],
  version: 1,
  virtualCoreCount: 1,
}
```

## `class com.dremio.provision.ClusterResponse`
- Example:
```
{
  clusterType: "YARN" | "MESOS" | "AWS" | "KUBERNETES" | "GCE" | "AZURE",
  containers: {
    decommissioningCount: 1,
    disconnectedList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            value: "abc",
          },
          ...
        ],
      },
      ...
    ],
    pendingCount: 1,
    provisioningCount: 1,
    runningList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            value: "abc",
          },
          ...
        ],
      },
      ...
    ],
  },
  currentState: "CREATED" | "STARTING" | "RUNNING" | "STOPPING" | "STOPPED" | "FAILED" | "DELETED",
  desiredState: "DELETED" | "RUNNING" | "STOPPED",
  detailedError: "abc",
  distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
  dynamicConfig: {
    containerCount: 1,
  },
  error: "abc",
  id: "abc",
  isSecure: true | false,
  memoryMB: 1,
  name: "abc",
  queue: "abc",
  subPropertyList: [
    { /** Property **/
      key: "abc",
      value: "abc",
    },
    ...
  ],
  version: 1,
  virtualCoreCount: 1,
}
```

## `class com.dremio.provision.ClusterResponses`
- Example:
```
{
  clusterList: [
    {
      clusterType: "YARN" | "MESOS" | "AWS" | "KUBERNETES" | "GCE" | "AZURE",
      containers: {
        decommissioningCount: 1,
        disconnectedList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                value: "abc",
              },
              ...
            ],
          },
          ...
        ],
        pendingCount: 1,
        provisioningCount: 1,
        runningList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                value: "abc",
              },
              ...
            ],
          },
          ...
        ],
      },
      currentState: "CREATED" | "STARTING" | "RUNNING" | "STOPPING" | "STOPPED" | "FAILED" | "DELETED",
      desiredState: "DELETED" | "RUNNING" | "STOPPED",
      detailedError: "abc",
      distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
      dynamicConfig: {
        containerCount: 1,
      },
      error: "abc",
      id: "abc",
      isSecure: true | false,
      memoryMB: 1,
      name: "abc",
      queue: "abc",
      subPropertyList: [
        { /** Property **/
          key: "abc",
          value: "abc",
        },
        ...
      ],
      version: 1,
      virtualCoreCount: 1,
    },
    ...
  ],
}
```

## `class com.dremio.provision.ResizeClusterRequest`
- Example:
```
{
  containerCount: 1,
}
```


#V2 Others: 
## class com.dremio.dac.explore.bi.QlikAppMessageBodyGenerator
## class com.dremio.dac.explore.bi.TableauMessageBodyGenerator
## class com.dremio.dac.server.DACAuthFilterFeature
## class com.dremio.dac.server.DACExceptionMapperFeature
## class com.dremio.dac.server.DACJacksonJaxbJsonFeature
## class com.dremio.dac.server.FirstTimeFeature
## class com.dremio.dac.server.JSONPrettyPrintFilter
## class com.dremio.dac.server.MediaTypeFilter
## class com.dremio.dac.server.TestResourcesFeature
## class org.glassfish.jersey.media.multipart.MultiPartFeature
## class org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature

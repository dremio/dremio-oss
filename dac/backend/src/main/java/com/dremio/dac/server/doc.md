# TOC
 - [Resources](#v2-resources)
 - [JobData](#v2-data)
 - [Others](#v2-others)

#V2 Resources: 
## Resource defined by class com.dremio.dac.server.TimingApplicationEventListener


## Resource defined by class com.dremio.dac.resource.BackupResource

 - POST /backup   
   > `=>` [com.dremio.dac.util.BackupRestoreUtil$BackupOptions](#class-comdremiodacutilbackuprestoreutil$backupoptions)   
   > `<=` [com.dremio.dac.util.BackupRestoreUtil$BackupStats](#class-comdremiodacutilbackuprestoreutil$backupstats)   


## Resource defined by class com.dremio.dac.explore.DatasetResource

 - DELETE /dataset/{cpath}?savedTag={String}   
   > `<=` [com.dremio.dac.explore.model.DatasetUI](#class-comdremiodacexploremodeldatasetui)   

 - GET /dataset/{cpath}   
   > `<=` [com.dremio.dac.explore.model.DatasetUI](#class-comdremiodacexploremodeldatasetui)   

 - POST /dataset/{cpath}/moveTo/{newpath} (path params: newpath={com.dremio.dac.explore.model.DatasetPath})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.DatasetUI](#class-comdremiodacexploremodeldatasetui)   

 - POST /dataset/{cpath}/rename?renameTo={String}   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.DatasetUI](#class-comdremiodacexploremodeldatasetui)   

 - GET /dataset/{cpath}/acceleration/settings   
   > `<=` [com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor](#class-comdremioservicenamespacephysicaldatasetprotoaccelerationsettingsdescriptor)   

 - PUT /dataset/{cpath}/acceleration/settings   
   > `=>` [com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor](#class-comdremioservicenamespacephysicaldatasetprotoaccelerationsettingsdescriptor)   
   > `<=` void   

 - PUT /dataset/{cpath}/copyFrom/{cpathFrom} (path params: cpathFrom={com.dremio.dac.explore.model.DatasetPath})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.DatasetUI](#class-comdremiodacexploremodeldatasetui)   

 - GET /dataset/{cpath}/descendants   
   > `<=` java.util.List`<`java.util.List`<`String`>``>`   

 - GET /dataset/{cpath}/descendants/count   
   > `<=` long   

 - GET /dataset/{cpath}/preview?limit={java.lang.Integer}50   
   > `<=` [com.dremio.dac.explore.model.InitialDataPreviewResponse](#class-comdremiodacexploremodelinitialdatapreviewresponse)   


## Resource defined by class com.dremio.dac.explore.DatasetVersionResource

 - GET /dataset/{cpath}/version/{version}   
   > `<=` [com.dremio.dac.explore.model.Dataset](#class-comdremiodacexploremodeldataset)   

 - POST /dataset/{cpath}/version/{version}/editOriginalSql   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /dataset/{cpath}/version/{version}/reapplyAndSave?as={com.dremio.dac.explore.model.DatasetPath}   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.DatasetUIWithHistory](#class-comdremiodacexploremodeldatasetuiwithhistory)   

 - POST /dataset/{cpath}/version/{version}/clean   
   > `=>` [com.dremio.dac.explore.model.ColumnForCleaning](#class-comdremiodacexploremodelcolumnforcleaning)   
   > `<=` [com.dremio.dac.explore.model.CleanDataCard](#class-comdremiodacexploremodelcleandatacard)   

 - POST /dataset/{cpath}/version/{version}/exclude   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards](#class-comdremiodacexploremodelextractreplacecards)   

 - POST /dataset/{cpath}/version/{version}/exclude_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule)`>`   

 - POST /dataset/{cpath}/version/{version}/exclude_values_preview   
   > `=>` [com.dremio.dac.explore.model.ReplaceValuesPreviewReq](#class-comdremiodacexploremodelreplacevaluespreviewreq)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards$ReplaceValuesCard](#class-comdremiodacexploremodelextractreplacecards$replacevaluescard)   

 - POST /dataset/{cpath}/version/{version}/extract   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` com.dremio.dac.explore.model.extract.Cards`<`[com.dremio.dac.proto.model.dataset.ExtractRule](#class-comdremiodacprotomodeldatasetextractrule)`>`   

 - POST /dataset/{cpath}/version/{version}/extract_list   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` com.dremio.dac.explore.model.extract.Cards`<`[com.dremio.dac.proto.model.dataset.ExtractListRule](#class-comdremiodacprotomodeldatasetextractlistrule)`>`   

 - POST /dataset/{cpath}/version/{version}/extract_list_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ExtractListRule](#class-comdremiodacprotomodeldatasetextractlistrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ExtractListRule](#class-comdremiodacprotomodeldatasetextractlistrule)`>`   

 - POST /dataset/{cpath}/version/{version}/extract_map   
   > `=>` [com.dremio.dac.explore.model.extract.MapSelection](#class-comdremiodacexploremodelextractmapselection)   
   > `<=` com.dremio.dac.explore.model.extract.Cards`<`[com.dremio.dac.proto.model.dataset.ExtractMapRule](#class-comdremiodacprotomodeldatasetextractmaprule)`>`   

 - POST /dataset/{cpath}/version/{version}/extract_map_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ExtractMapRule](#class-comdremiodacprotomodeldatasetextractmaprule), [com.dremio.dac.explore.model.extract.MapSelection](#class-comdremiodacexploremodelextractmapselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ExtractMapRule](#class-comdremiodacprotomodeldatasetextractmaprule)`>`   

 - POST /dataset/{cpath}/version/{version}/extract_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ExtractRule](#class-comdremiodacprotomodeldatasetextractrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ExtractRule](#class-comdremiodacprotomodeldatasetextractrule)`>`   

 - GET /dataset/{cpath}/version/{version}/history?tipVersion={com.dremio.service.namespace.dataset.DatasetVersion}   
   > `<=` [com.dremio.dac.explore.model.History](#class-comdremiodacexploremodelhistory)   

 - GET /dataset/{cpath}/version/{version}/join_recs   
   > `<=` [com.dremio.dac.explore.model.JoinRecommendations](#class-comdremiodacexploremodeljoinrecommendations)   

 - POST /dataset/{cpath}/version/{version}/keeponly   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards](#class-comdremiodacexploremodelextractreplacecards)   

 - POST /dataset/{cpath}/version/{version}/keeponly_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule)`>`   

 - POST /dataset/{cpath}/version/{version}/keeponly_values_preview   
   > `=>` [com.dremio.dac.explore.model.ReplaceValuesPreviewReq](#class-comdremiodacexploremodelreplacevaluespreviewreq)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards$ReplaceValuesCard](#class-comdremiodacexploremodelextractreplacecards$replacevaluescard)   

 - GET /dataset/{cpath}/version/{version}/parents   
   > `<=` java.util.List`<`[com.dremio.dac.explore.model.ParentDatasetUI](#class-comdremiodacexploremodelparentdatasetui)`>`   

 - GET /dataset/{cpath}/version/{version}/preview?tipVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={java.lang.Integer}   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /dataset/{cpath}/version/{version}/replace   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards](#class-comdremiodacexploremodelextractreplacecards)   

 - POST /dataset/{cpath}/version/{version}/replace_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.ReplacePatternRule](#class-comdremiodacprotomodeldatasetreplacepatternrule)`>`   

 - POST /dataset/{cpath}/version/{version}/replace_values_preview   
   > `=>` [com.dremio.dac.explore.model.ReplaceValuesPreviewReq](#class-comdremiodacexploremodelreplacevaluespreviewreq)   
   > `<=` [com.dremio.dac.explore.model.extract.ReplaceCards$ReplaceValuesCard](#class-comdremiodacexploremodelextractreplacecards$replacevaluescard)   

 - GET /dataset/{cpath}/version/{version}/review?jobId={String}&tipVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={java.lang.Integer}   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - GET /dataset/{cpath}/version/{version}/run?tipVersion={com.dremio.service.namespace.dataset.DatasetVersion}   
   > `<=` [com.dremio.dac.explore.model.InitialRunResponse](#class-comdremiodacexploremodelinitialrunresponse)   

 - POST /dataset/{cpath}/version/{version}/save?as={com.dremio.dac.explore.model.DatasetPath}&savedTag={String}   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.DatasetUIWithHistory](#class-comdremiodacexploremodeldatasetuiwithhistory)   

 - POST /dataset/{cpath}/version/{version}/split   
   > `=>` [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)   
   > `<=` com.dremio.dac.explore.model.extract.Cards`<`[com.dremio.dac.proto.model.dataset.SplitRule](#class-comdremiodacprotomodeldatasetsplitrule)`>`   

 - POST /dataset/{cpath}/version/{version}/split_preview   
   > `=>` com.dremio.dac.explore.model.PreviewReq`<`[com.dremio.dac.proto.model.dataset.SplitRule](#class-comdremiodacprotomodeldatasetsplitrule), [com.dremio.dac.explore.model.extract.Selection](#class-comdremiodacexploremodelextractselection)`>`   
   > `<=` com.dremio.dac.explore.model.extract.Card`<`[com.dremio.dac.proto.model.dataset.SplitRule](#class-comdremiodacprotomodeldatasetsplitrule)`>`   

 - POST /dataset/{cpath}/version/{version}/transformAndPreview?newVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={int}50   
   > `=>` [com.dremio.dac.explore.model.TransformBase](#class-comdremiodacexploremodeltransformbase)   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /dataset/{cpath}/version/{version}/transformAndRun?newVersion={com.dremio.service.namespace.dataset.DatasetVersion}   
   > `=>` [com.dremio.dac.explore.model.TransformBase](#class-comdremiodacexploremodeltransformbase)   
   > `<=` [com.dremio.dac.explore.model.InitialTransformAndRunResponse](#class-comdremiodacexploremodelinitialtransformandrunresponse)   

 - POST /dataset/{cpath}/version/{version}/transformPeek?newVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={int}50   
   > `=>` [com.dremio.dac.explore.model.TransformBase](#class-comdremiodacexploremodeltransformbase)   
   > `<=` [com.dremio.dac.explore.model.InitialPendingTransformResponse](#class-comdremiodacexploremodelinitialpendingtransformresponse)   


## Resource defined by class com.dremio.dac.explore.DatasetsResource

 - GET /datasets/context/{type}/{datasetContainer}/{path: .*} (path params: type={String}, datasetContainer={String}, path={String})   
   > `<=` [com.dremio.dac.explore.model.DatasetDetails](#class-comdremiodacexploremodeldatasetdetails)   

 - GET /datasets/summary/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.DatasetSummary](#class-comdremiodacexploremodeldatasetsummary)   

 - POST /datasets/new_untitled?parentDataset={com.dremio.dac.explore.model.DatasetPath}&newVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={java.lang.Integer}   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /datasets/new_untitled_sql?newVersion={com.dremio.service.namespace.dataset.DatasetVersion}&limit={java.lang.Integer}   
   > `=>` [com.dremio.dac.explore.model.CreateFromSQL](#class-comdremiodacexploremodelcreatefromsql)   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /datasets/new_untitled_sql_and_run?newVersion={com.dremio.service.namespace.dataset.DatasetVersion}   
   > `=>` [com.dremio.dac.explore.model.CreateFromSQL](#class-comdremiodacexploremodelcreatefromsql)   
   > `<=` [com.dremio.dac.explore.model.InitialRunResponse](#class-comdremiodacexploremodelinitialrunresponse)   

 - GET /datasets/search?filter={String}&sort={String}&order={com.dremio.datastore.SearchTypes$SortOrder}   
   > `<=` [com.dremio.dac.explore.model.DatasetSearchUIs](#class-comdremiodacexploremodeldatasetsearchuis)   


## Resource defined by class com.dremio.dac.resource.DevelopmentOptionsResource

 - POST /development_options/acceleration/clearall   
   > `=>`   
   > `<=` void   

 - GET /development_options/acceleration/enabled   
   > `<=` String   

 - PUT /development_options/acceleration/enabled   
   > `=>` String   
   > `<=` String   

 - GET /development_options/acceleration/settings   
   > `<=` [com.dremio.dac.proto.model.acceleration.SystemSettingsApiDescriptor](#class-comdremiodacprotomodelaccelerationsystemsettingsapidescriptor)   

 - PUT /development_options/acceleration/settings   
   > `=>` [com.dremio.dac.proto.model.acceleration.SystemSettingsApiDescriptor](#class-comdremiodacprotomodelaccelerationsystemsettingsapidescriptor)   
   > `<=` void   


## Resource defined by class com.dremio.dac.resource.ExportProfilesResource

 - POST /export-profiles   
   > `=>` [com.dremio.dac.resource.ExportProfilesParams](#class-comdremiodacresourceexportprofilesparams)   
   > `<=` [com.dremio.dac.resource.ExportProfilesStats](#class-comdremiodacresourceexportprofilesstats)   


## Resource defined by class com.dremio.dac.resource.HomeResource

 - GET /home/{homeName}?includeContents={boolean}true   
   > `<=` [com.dremio.dac.model.spaces.Home](#class-comdremiodacmodelspaceshome)   

 - DELETE /home/{homeName}/folder/{path: .*}?version={String} (path params: path={String})   
   > `<=` void   

 - GET /home/{homeName}/folder/{path: .*}?includeContents={boolean}true (path params: path={String})   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   

 - POST /home/{homeName}/folder/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.dac.model.folder.FolderName](#class-comdremiodacmodelfolderfoldername)   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   

 - POST /home/{homeName}/new_untitled_from_file/{path: .*}?limit={java.lang.Integer} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - GET /home/{homeName}/dataset/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.Dataset](#class-comdremiodacexploremodeldataset)   

 - DELETE /home/{homeName}/file/{path: .*}?version={String} (path params: path={String})   
   > `<=` void   

 - GET /home/{homeName}/file/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.file.File](#class-comdremiofilefile)   

 - GET /home/{homeName}/file_format/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - PUT /home/{homeName}/file_format/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - POST /home/{homeName}/file_preview/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - POST /home/{homeName}/file_preview_unsaved/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - POST /home/{homeName}/file_rename/{path: .*}?renameTo={com.dremio.file.FileName} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.file.File](#class-comdremiofilefile)   

 - POST /home/{homeName}/upload_cancel/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` void   

 - POST /home/{homeName}/upload_finish/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.file.File](#class-comdremiofilefile)   

 - POST /home/{homeName}/upload_start/{path: .*}?extension={String} (path params: path={String})   
   > `=>`   
   > file={java.io.InputStream}   
   > file={org.glassfish.jersey.media.multipart.FormDataContentDisposition}   
   > fileName={com.dremio.file.FileName}   
   > `<=` [com.dremio.file.File](#class-comdremiofilefile)   


## Resource defined by class com.dremio.dac.resource.JobResource

 - GET /job/{jobId} (path params: jobId={String})   
   > `<=` [com.dremio.dac.model.job.JobUI](#class-comdremiodacmodeljobjobui)   

 - GET /job/{jobId}/data?limit={int}&offset={int} (path params: jobId={com.dremio.service.job.proto.JobId})   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - GET /job/{jobId}/details (path params: jobId={String})   
   > `<=` [com.dremio.dac.model.job.JobDetailsUI](#class-comdremiodacmodeljobjobdetailsui)   

 - GET /job/{jobId}/r/{rowNum}/c/{columnName} (path params: jobId={com.dremio.service.job.proto.JobId}, rowNum={int}, columnName={String})   
   > `<=` java.lang.Object   

 - POST /job/{jobId}/reflection/{reflectionId}/cancel (path params: jobId={String}, reflectionId={String})   
   > `=>`   
   > `<=` [com.dremio.dac.resource.NotificationResponse](#class-comdremiodacresourcenotificationresponse)   

 - GET /job/{jobId}/reflection/{reflectionId}/details (path params: jobId={String}, reflectionId={String})   
   > `<=` [com.dremio.dac.model.job.JobDetailsUI](#class-comdremiodacmodeljobjobdetailsui)   

 - POST /job/{jobId}/cancel (path params: jobId={String})   
   > `=>`   
   > `<=` [com.dremio.dac.resource.NotificationResponse](#class-comdremiodacresourcenotificationresponse)   

 - GET /job/{jobId}/download?downloadFormat={com.dremio.dac.explore.model.DownloadFormat} (path params: jobId={com.dremio.service.job.proto.JobId})   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.JobsResource

 - GET /jobs?filter={String}&sort={String}&order={com.dremio.dac.model.job.ResultOrder}&offset={int}0&limit={int}100   
   > `<=` [com.dremio.dac.model.job.JobsUI](#class-comdremiodacmodeljobjobsui)   

 - GET /jobs/reflection/{reflectionId}?offset={int}0&limit={int}100 (path params: reflectionId={String})   
   > `<=` [com.dremio.dac.model.job.JobsUI](#class-comdremiodacmodeljobjobsui)   


## Resource defined by class com.dremio.dac.resource.JobsFiltersResource

 - GET /jobs/filters/spaces?filter={String}&limit={java.lang.Integer}   
   > `<=` [com.dremio.dac.model.job.JobFilterItems](#class-comdremiodacmodeljobjobfilteritems)   

 - GET /jobs/filters/users?filter={String}&limit={java.lang.Integer}   
   > `<=` [com.dremio.dac.model.job.JobFilterItems](#class-comdremiodacmodeljobjobfilteritems)   


## Resource defined by class com.dremio.dac.resource.LogInLogOutResource

 - DELETE /login   
   > `=>`   
   > Authorization: {String}   
   > `<=` void   

 - GET /login   
   > `<=` boolean   

 - POST /login   
   > `=>` [com.dremio.dac.model.usergroup.UserLogin](#class-comdremiodacmodelusergroupuserlogin)   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.PowerBIResource

 - GET /powerbi/{datasetId}   
   > `=>`   
   > Host: {String}   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.admin.ProfileResource

 - GET /profiles/cancel/{queryid} (path params: queryid={String})   
   > `<=` [com.dremio.dac.resource.NotificationResponse](#class-comdremiodacresourcenotificationresponse)   

 - GET /profiles/cancel/{queryid}/reflection/{reflectionId} (path params: queryid={String}, reflectionId={String})   
   > `<=` [com.dremio.dac.resource.NotificationResponse](#class-comdremiodacresourcenotificationresponse)   

 - GET /profiles/reflection/{reflectionId}/{queryid}.json?attempt={int}0 (path params: queryid={String}, reflectionId={String})   
   > `<=` String   

 - GET /profiles/{queryid}?attempt={int}0 (path params: queryid={String})   
   > `<=` [org.glassfish.jersey.server.mvc.Viewable](#class-orgglassfishjerseyservermvcviewable)   

 - GET /profiles/{queryid}.json?attempt={int}0 (path params: queryid={String})   
   > `<=` String   

 - GET /profiles/{queryid}/reflection/{reflectionId}?attempt={int}0 (path params: queryid={String}, reflectionId={String})   
   > `<=` [org.glassfish.jersey.server.mvc.Viewable](#class-orgglassfishjerseyservermvcviewable)   


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


## Resource defined by class com.dremio.dac.explore.bi.QlikResource

 - GET /qlik/{datasetId}   
   > `=>`   
   > Host: {String}   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.ResourceTreeResource

 - GET /resourcetree?showSpaces={boolean}&showSources={boolean}&showHomes={boolean}   
   > `<=` [com.dremio.dac.model.resourcetree.ResourceList](#class-comdremiodacmodelresourcetreeresourcelist)   

 - GET /resourcetree/{rootPath}/expand?showSpaces={boolean}&showSources={boolean}&showDatasets={boolean}&showHomes={boolean} (path params: rootPath={String})   
   > `<=` [com.dremio.dac.model.resourcetree.ResourceList](#class-comdremiodacmodelresourcetreeresourcelist)   

 - GET /resourcetree/{rootPath}?showDatasets={boolean} (path params: rootPath={String})   
   > `<=` [com.dremio.dac.model.resourcetree.ResourceList](#class-comdremiodacmodelresourcetreeresourcelist)   


## Resource defined by class com.dremio.dac.resource.ServerStatusResource

 - GET /server_status   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.service.admin.SettingsResource

 - POST /settings   
   > `=>` [com.dremio.dac.service.admin.SettingsResource$SettingsRequest](#class-comdremiodacserviceadminsettingsresource$settingsrequest)   
   > `<=` [com.dremio.dac.service.admin.SettingsResource$SettingsWrapperObject](#class-comdremiodacserviceadminsettingsresource$settingswrapperobject)   

 - DELETE /settings/{id} (path params: id={String})   
   > `<=` javax.ws.rs.core.Response   

 - GET /settings/{id} (path params: id={String})   
   > `<=` javax.ws.rs.core.Response   

 - PUT /settings/{id} (path params: id={String})   
   > `=>` [com.dremio.dac.service.admin.Setting](#class-comdremiodacserviceadminsetting)   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.PutSourceResource

 - PUT /source/{sourceName}   
   > `=>` [com.dremio.dac.model.sources.SourceUI](#class-comdremiodacmodelsourcessourceui)   
   > `<=` [com.dremio.dac.model.sources.SourceUI](#class-comdremiodacmodelsourcessourceui)   


## Resource defined by class com.dremio.dac.resource.SourceResource

 - DELETE /source/{sourceName}?version={String}   
   > `<=` void   

 - GET /source/{sourceName}?includeContents={boolean}true   
   > `<=` [com.dremio.dac.model.sources.SourceUI](#class-comdremiodacmodelsourcessourceui)   

 - GET /source/{sourceName}/dataset/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.model.sources.PhysicalDataset](#class-comdremiodacmodelsourcesphysicaldataset)   

 - GET /source/{sourceName}/file/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.file.File](#class-comdremiofilefile)   

 - DELETE /source/{sourceName}/file_format/{path: .*}?version={String} (path params: path={String})   
   > `<=` void   

 - GET /source/{sourceName}/file_format/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - PUT /source/{sourceName}/file_format/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - POST /source/{sourceName}/file_preview/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - GET /source/{sourceName}/folder/{path: .*}?includeContents={boolean}true (path params: path={String})   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   

 - DELETE /source/{sourceName}/folder_format/{path: .*}?version={String} (path params: path={String})   
   > `<=` void   

 - GET /source/{sourceName}/folder_format/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - PUT /source/{sourceName}/folder_format/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.explore.model.FileFormatUI](#class-comdremiodacexploremodelfileformatui)   

 - POST /source/{sourceName}/folder_preview/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.service.namespace.file.FileFormat](#class-comdremioservicenamespacefilefileformat)   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - POST /source/{sourceName}/new_untitled_from_file/{path: .*}?limit={java.lang.Integer} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /source/{sourceName}/new_untitled_from_folder/{path: .*}?limit={java.lang.Integer} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   

 - POST /source/{sourceName}/new_untitled_from_physical_dataset/{path: .*}?limit={java.lang.Integer} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.dac.explore.model.InitialPreviewResponse](#class-comdremiodacexploremodelinitialpreviewresponse)   


## Resource defined by class com.dremio.dac.resource.SourcesResource

 - GET /sources   
   > `<=` [com.dremio.dac.model.sources.Sources](#class-comdremiodacmodelsourcessources)   

 - POST /sources/isMetadataImpacting   
   > `=>` [com.dremio.dac.model.sources.SourceUI](#class-comdremiodacmodelsourcessourceui)   
   > `<=` [com.dremio.dac.resource.SourcesResource$MetadataImpactingResponse](#class-comdremiodacresourcesourcesresource$metadataimpactingresponse)   


## Resource defined by class com.dremio.dac.resource.PutSpaceResource

 - PUT /space/{spaceName}   
   > `=>` [com.dremio.dac.model.spaces.Space](#class-comdremiodacmodelspacesspace)   
   > `<=` [com.dremio.dac.model.spaces.Space](#class-comdremiodacmodelspacesspace)   


## Resource defined by class com.dremio.dac.resource.SpaceResource

 - DELETE /space/{spaceName}?version={String}   
   > `<=` void   

 - GET /space/{spaceName}?includeContents={boolean}true   
   > `<=` [com.dremio.dac.model.spaces.Space](#class-comdremiodacmodelspacesspace)   

 - POST /space/{spaceName}/rename?renameTo={String}   
   > `=>`   
   > `<=` [com.dremio.dac.model.spaces.Space](#class-comdremiodacmodelspacesspace)   

 - GET /space/{spaceName}/dataset/{path: .*} (path params: path={String})   
   > `<=` [com.dremio.dac.explore.model.Dataset](#class-comdremiodacexploremodeldataset)   


## Resource defined by class com.dremio.dac.resource.FolderResource

 - DELETE /space/{space}/folder/{path: .*}?version={String} (path params: path={String})   
   > `<=` void   

 - GET /space/{space}/folder/{path: .*}?includeContents={boolean}true (path params: path={String})   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   

 - POST /space/{space}/folder/{path: .*} (path params: path={String})   
   > `=>` [com.dremio.dac.model.folder.FolderName](#class-comdremiodacmodelfolderfoldername)   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   

 - POST /space/{space}/rename_folder/{path: .*}?renameTo={String} (path params: path={String})   
   > `=>`   
   > `<=` [com.dremio.dac.model.folder.Folder](#class-comdremiodacmodelfolderfolder)   


## Resource defined by class com.dremio.dac.resource.SpacesResource

 - GET /spaces   
   > `<=` [com.dremio.dac.model.spaces.Spaces](#class-comdremiodacmodelspacesspaces)   


## Resource defined by class com.dremio.dac.resource.SQLResource

 - POST /sql   
   > `=>` [com.dremio.dac.explore.model.CreateFromSQL](#class-comdremiodacexploremodelcreatefromsql)   
   > `<=` [com.dremio.dac.model.job.JobDataFragment](#class-comdremiodacmodeljobjobdatafragment)   

 - POST /sql/analyze/suggest   
   > `=>` [com.dremio.dac.explore.model.AnalyzeRequest](#class-comdremiodacexploremodelanalyzerequest)   
   > `<=` [com.dremio.dac.explore.model.SuggestionResponse](#class-comdremiodacexploremodelsuggestionresponse)   

 - POST /sql/analyze/validate   
   > `=>` [com.dremio.dac.explore.model.AnalyzeRequest](#class-comdremiodacexploremodelanalyzerequest)   
   > `<=` [com.dremio.dac.explore.model.ValidationResponse](#class-comdremiodacexploremodelvalidationresponse)   


## Resource defined by class com.dremio.dac.support.SupportResource

 - POST /support/{jobId} (path params: jobId={com.dremio.service.job.proto.JobId})   
   > `=>`   
   > `<=` [com.dremio.dac.support.SupportResponse](#class-comdremiodacsupportsupportresponse)   

 - POST /support/{jobId}/download (path params: jobId={com.dremio.service.job.proto.JobId})   
   > `=>`   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.SystemResource

 - GET /system/cluster-resource-info   
   > `<=` [com.dremio.dac.resource.SystemResource$ResourceInfo](#class-comdremiodacresourcesystemresource$resourceinfo)   

 - GET /system/nodes   
   > `<=` java.util.List`<`[com.dremio.dac.model.system.Nodes$NodeInfo](#class-comdremiodacmodelsystemnodes$nodeinfo)`>`   


## Resource defined by class com.dremio.dac.resource.TableauResource

 - GET /tableau/{datasetId}   
   > `=>`   
   > Host: {String}   
   > `<=` javax.ws.rs.core.Response   


## Resource defined by class com.dremio.dac.resource.UserResource

 - DELETE /user/{userName}?version={String} (path params: userName={com.dremio.dac.model.usergroup.UserName})   
   > `<=` javax.ws.rs.core.Response   

 - GET /user/{userName} (path params: userName={com.dremio.dac.model.usergroup.UserName})   
   > `<=` [com.dremio.dac.model.usergroup.UserUI](#class-comdremiodacmodelusergroupuserui)   

 - POST /user/{userName} (path params: userName={com.dremio.dac.model.usergroup.UserName})   
   > `=>` [com.dremio.dac.model.usergroup.UserForm](#class-comdremiodacmodelusergroupuserform)   
   > `<=` [com.dremio.dac.model.usergroup.UserUI](#class-comdremiodacmodelusergroupuserui)   

 - PUT /user/{userName} (path params: userName={com.dremio.dac.model.usergroup.UserName})   
   > `=>` [com.dremio.dac.model.usergroup.UserForm](#class-comdremiodacmodelusergroupuserform)   
   > `<=` [com.dremio.dac.model.usergroup.UserUI](#class-comdremiodacmodelusergroupuserui)   


## Resource defined by class com.dremio.dac.resource.UsersResource

 - GET /users/all   
   > `<=` [com.dremio.dac.model.usergroup.UsersUI](#class-comdremiodacmodelusergroupusersui)   

 - GET /users/search?filter={String}   
   > `<=` [com.dremio.dac.model.usergroup.UsersUI](#class-comdremiodacmodelusergroupusersui)   



#V2 JobData
## `class com.dremio.dac.explore.model.AnalyzeRequest`
- Example:
```
{
  context: [
    "abc",
    ...
  ],
  cursorPosition: 1,
  sql: "abc",
}
```

## `class com.dremio.dac.explore.model.CleanDataCard`
- Example:
```
{
  availableValues: [
    { /** HistogramValue **/
      count: 1,
      percent: 1.0,
      type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      value: "abc",
      valueRange: {
        lowerLimit: any,
        upperLimit: any,
      },
    },
    ...
  ],
  availableValuesCount: 1,
  convertToSingles: [
    {
      availableNonMatching: [
        { /** HistogramValue **/
          count: 1,
          percent: 1.0,
          type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
          value: "abc",
          valueRange: {
            lowerLimit: any,
            upperLimit: any,
          },
        },
        ...
      ],
      castWhenPossible: true | false,
      desiredType: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      nonMatchingCount: 1,
    },
    ...
  ],
  newFieldName: "abc",
  newFieldNamePrefix: "abc",
  split: [
    {
      matchingPercent: 1.0,
      type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
    },
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.ColumnForCleaning`
- Example:
```
{
  colName: "abc",
}
```

## `class com.dremio.dac.explore.model.CreateFromSQL`
- Example:
```
{
  context: [
    "abc",
    ...
  ],
  sql: "abc",
}
```

## `class com.dremio.dac.explore.model.Dataset`
- Example:
```
{
  datasetConfig: {
    accelerated: true | false,
    calciteFieldsList: [
      { /** ViewFieldType **/
        endUnit: "abc",
        fractionalSecondPrecision: 1,
        isNullable: true | false,
        name: "abc",
        precision: 1,
        scale: 1,
        startUnit: "abc",
        type: "abc",
        typeFamily: "abc",
      },
      ...
    ],
    contextList: [
      "abc",
      ...
    ],
    createdAt: 1,
    derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
    fieldOriginsList: [
      {
        name: "abc",
        originsList: [
          {
            columnName: "abc",
            derived: true | false,
            tableList: [
              "abc",
              ...
            ],
          },
          ...
        ],
      },
      ...
    ],
    fullPathList: [
      "abc",
      ...
    ],
    grandParentsList: [
      { /** ParentDataset **/
        datasetPathList: [
          "abc",
          ...
        ],
        level: 1,
        type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
      },
      ...
    ],
    id: "abc",
    isNamed: true | false,
    lastTransform: any,
    name: "abc",
    owner: "abc",
    parentPath: "abc",
    parentsList: [
      { /** ParentDataset **/
        datasetPathList: [
          "abc",
          ...
        ],
        level: 1,
        type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
      },
      ...
    ],
    previousVersion: {
      datasetPath: "abc",
      datasetVersion: "abc",
    },
    recordSchema: {
      empty: true | false,
    },
    savedTag: "abc",
    sql: "abc",
    sqlFieldsList: [
      { /** ViewFieldType **/
        endUnit: "abc",
        fractionalSecondPrecision: 1,
        isNullable: true | false,
        name: "abc",
        precision: 1,
        scale: 1,
        startUnit: "abc",
        type: "abc",
        typeFamily: "abc",
      },
      ...
    ],
    state: {
      columnsList: [
        { /** Column **/
          name: "abc",
          value: any,
        },
        ...
      ],
      contextList: [
        "abc",
        ...
      ],
      filtersList: [
        {
          exclude: true | false,
          filterDef: any,
          keepNull: true | false,
          operand: any,
        },
        ...
      ],
      from: any,
      groupBysList: [
        { /** Column **/
          name: "abc",
          value: any,
        },
        ...
      ],
      joinsList: [
        {
          joinAlias: "abc",
          joinConditionsList: [
            {
              leftColumn: "abc",
              rightColumn: "abc",
            },
            ...
          ],
          joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
          rightTable: "abc",
        },
        ...
      ],
      ordersList: [
        {
          direction: "ASC" | "DESC",
          name: "abc",
        },
        ...
      ],
      referredTablesList: [
        "abc",
        ...
      ],
    },
    version: "abc",
  },
  datasetName: "abc",
  id: "abc",
  jobCount: 1,
  lastHistoryItem: {
    bytes: 1,
    createdAt: 1,
    datasetVersion: "abc",
    finishedAt: 1,
    owner: "abc",
    preview: true | false,
    recordsReturned: 1,
    state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
    transformDescription: "abc",
    versionedResourcePath: "abc",
  },
  links: {
    abc: "abc", ...
  },
  resourcePath: "abc",
  sql: "abc",
  tags: [
    "abc",
    ...
  ],
  versionedResourcePath: "abc",
}
```

## `class com.dremio.dac.explore.model.DatasetDetails`
- Example:
```
{
  createdAt: 1,
  descendants: 1,
  id: [
    "abc",
    ...
  ],
  jobCount: 1,
  owner: "abc",
  parentDatasetContainer: {
    ctime: 1,
    description: "abc",
    name: "abc",
  },
}
```

## `class com.dremio.dac.explore.model.DatasetSearchUIs`
- Example:
```
[
  {
    apiLinks: {
      abc: "abc", ...
    },
    context: [
      "abc",
      ...
    ],
    datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    displayFullPath: [
      "abc",
      ...
    ],
    fields: [
      {
        name: "abc",
        type: "abc",
      },
      ...
    ],
    fullPath: [
      "abc",
      ...
    ],
    links: {
      abc: "abc", ...
    },
    parents: [
      {
        datasetPathList: [
          "abc",
          ...
        ],
        level: 1,
        type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
      },
      ...
    ],
    tags: [
      "abc",
      ...
    ],
  },
  ...
]
```

## `class com.dremio.dac.explore.model.DatasetSummary`
- Example:
```
{
  apiLinks: {
    abc: "abc", ...
  },
  datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
  datasetVersion: "abc",
  descendants: 1,
  fields: [
    {
      isPartitioned: true | false,
      isSorted: true | false,
      name: "abc",
      type: "abc",
    },
    ...
  ],
  fullPath: [
    "abc",
    ...
  ],
  jobCount: 1,
  links: {
    abc: "abc", ...
  },
}
```

## `class com.dremio.dac.explore.model.DatasetUI`
- Example:
```
{
  apiLinks: {
    abc: "abc", ...
  },
  canReapply: true | false,
  context: [
    "abc",
    ...
  ],
  datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
  datasetVersion: "abc",
  descendants: 1,
  displayFullPath: [
    "abc",
    ...
  ],
  entityId: "abc",
  fullPath: [
    "abc",
    ...
  ],
  id: "abc",
  jobCount: 1,
  links: {
    abc: "abc", ...
  },
  sql: "abc",
  version: "abc",
}
```

## `class com.dremio.dac.explore.model.DatasetUIWithHistory`
- Example:
```
{
  dataset: {
    apiLinks: {
      abc: "abc", ...
    },
    canReapply: true | false,
    context: [
      "abc",
      ...
    ],
    datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    datasetVersion: "abc",
    descendants: 1,
    displayFullPath: [
      "abc",
      ...
    ],
    entityId: "abc",
    fullPath: [
      "abc",
      ...
    ],
    id: "abc",
    jobCount: 1,
    links: {
      abc: "abc", ...
    },
    sql: "abc",
    version: "abc",
  },
  history: {
    currentDatasetVersion: "abc",
    isEdited: true | false,
    items: [
      {
        bytes: 1,
        createdAt: 1,
        datasetVersion: "abc",
        finishedAt: 1,
        owner: "abc",
        preview: true | false,
        recordsReturned: 1,
        state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
        transformDescription: "abc",
        versionedResourcePath: "abc",
      },
      ...
    ],
    tipVersion: "abc",
  },
}
```

## `class com.dremio.dac.explore.model.FileFormatUI`
- Example:
```
{
  fileFormat: {
    ctime: 1,
    fullPath: [
      "abc",
      ...
    ],
    isFolder: true | false,
    location: "abc",
    name: "abc",
    owner: "abc",
    version: "abc",
  },
  id: "abc",
  links: {
    abc: "abc", ...
  },
}
```

## `class com.dremio.dac.explore.model.History`
- Example:
```
{
  currentDatasetVersion: "abc",
  isEdited: true | false,
  items: [
    {
      bytes: 1,
      createdAt: 1,
      datasetVersion: "abc",
      finishedAt: 1,
      owner: "abc",
      preview: true | false,
      recordsReturned: 1,
      state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
      transformDescription: "abc",
      versionedResourcePath: "abc",
    },
    ...
  ],
  tipVersion: "abc",
}
```

## `class com.dremio.dac.explore.model.InitialDataPreviewResponse`
- Example:
```
{
  data: {
    columns: [
      {
        index: 1,
        name: "abc",
        type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      },
      ...
    ],
    returnedRowCount: 1,
  },
  paginationUrl: "abc",
}
```

## `class com.dremio.dac.explore.model.InitialPendingTransformResponse`
- Example:
```
{
  data: {
    columns: [
      {
        index: 1,
        name: "abc",
        type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      },
      ...
    ],
    returnedRowCount: 1,
  },
  deletedColumns: [
    "abc",
    ...
  ],
  highlightedColumns: [
    "abc",
    ...
  ],
  paginationUrl: "abc",
  rowDeletionMarkerColumns: [
    "abc",
    ...
  ],
  sql: "abc",
}
```

## `class com.dremio.dac.explore.model.InitialPreviewResponse`
- Example:
```
{
  approximate: true | false,
  data: {
    columns: [
      {
        index: 1,
        name: "abc",
        type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      },
      ...
    ],
    returnedRowCount: 1,
  },
  dataset: {
    apiLinks: {
      abc: "abc", ...
    },
    canReapply: true | false,
    context: [
      "abc",
      ...
    ],
    datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    datasetVersion: "abc",
    descendants: 1,
    displayFullPath: [
      "abc",
      ...
    ],
    entityId: "abc",
    fullPath: [
      "abc",
      ...
    ],
    id: "abc",
    jobCount: 1,
    links: {
      abc: "abc", ...
    },
    sql: "abc",
    version: "abc",
  },
  error: {
    code: "INITIAL_PREVIEW_ERROR" | "NEW_DATASET_QUERY_EXCEPTION" | "INVALID_QUERY",
    details: any,
    errorMessage: "abc",
    moreInfo: "abc",
    stackTrace: [
      "abc",
      ...
    ],
  },
  history: {
    currentDatasetVersion: "abc",
    isEdited: true | false,
    items: [
      {
        bytes: 1,
        createdAt: 1,
        datasetVersion: "abc",
        finishedAt: 1,
        owner: "abc",
        preview: true | false,
        recordsReturned: 1,
        state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
        transformDescription: "abc",
        versionedResourcePath: "abc",
      },
      ...
    ],
    tipVersion: "abc",
  },
  jobId: {
    id: "abc",
    name: "abc",
  },
  paginationUrl: "abc",
}
```

## `class com.dremio.dac.explore.model.InitialRunResponse`
- Example:
```
{
  approximate: true | false,
  dataset: {
    apiLinks: {
      abc: "abc", ...
    },
    canReapply: true | false,
    context: [
      "abc",
      ...
    ],
    datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    datasetVersion: "abc",
    descendants: 1,
    displayFullPath: [
      "abc",
      ...
    ],
    entityId: "abc",
    fullPath: [
      "abc",
      ...
    ],
    id: "abc",
    jobCount: 1,
    links: {
      abc: "abc", ...
    },
    sql: "abc",
    version: "abc",
  },
  history: {
    currentDatasetVersion: "abc",
    isEdited: true | false,
    items: [
      {
        bytes: 1,
        createdAt: 1,
        datasetVersion: "abc",
        finishedAt: 1,
        owner: "abc",
        preview: true | false,
        recordsReturned: 1,
        state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
        transformDescription: "abc",
        versionedResourcePath: "abc",
      },
      ...
    ],
    tipVersion: "abc",
  },
  jobId: {
    id: "abc",
    name: "abc",
  },
  paginationUrl: "abc",
}
```

## `class com.dremio.dac.explore.model.InitialTransformAndRunResponse`
- Example:
```
{
  dataset: {
    apiLinks: {
      abc: "abc", ...
    },
    canReapply: true | false,
    context: [
      "abc",
      ...
    ],
    datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    datasetVersion: "abc",
    descendants: 1,
    displayFullPath: [
      "abc",
      ...
    ],
    entityId: "abc",
    fullPath: [
      "abc",
      ...
    ],
    id: "abc",
    jobCount: 1,
    links: {
      abc: "abc", ...
    },
    sql: "abc",
    version: "abc",
  },
  history: {
    currentDatasetVersion: "abc",
    isEdited: true | false,
    items: [
      {
        bytes: 1,
        createdAt: 1,
        datasetVersion: "abc",
        finishedAt: 1,
        owner: "abc",
        preview: true | false,
        recordsReturned: 1,
        state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
        transformDescription: "abc",
        versionedResourcePath: "abc",
      },
      ...
    ],
    tipVersion: "abc",
  },
  jobId: {
    id: "abc",
    name: "abc",
  },
  paginationUrl: "abc",
}
```

## `class com.dremio.dac.explore.model.JoinRecommendations`
- Example:
```
{
  recommendations: [
    {
      joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
      links: {
        abc: "abc", ...
      },
      matchingKeys: {
        abc: "abc", ...
      },
      rightTableFullPathList: [
        "abc",
        ...
      ],
    },
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.ParentDatasetUI`
- Example:
```
{
  datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
  fullPath: [
    "abc",
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.ReplaceValuesPreviewReq`
- Example:
```
{
  replaceNull: true | false,
  replacedValues: [
    "abc",
    ...
  ],
  selection: {
    cellText: "abc",
    colName: "abc",
    length: 1,
    offset: 1,
  },
}
```

## `class com.dremio.dac.explore.model.SuggestionResponse`
- Example:
```
{
  suggestions: [
    {
      name: "abc",
      type: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.TransformBase`
- Example:
```
{
}
```

## `class com.dremio.dac.explore.model.ValidationResponse`
- Example:
```
{
  errors: [
    {
      message: "abc",
      range: {
        endColumn: 1,
        endLine: 1,
        startColumn: 1,
        startLine: 1,
      },
    },
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.extract.MapSelection`
- Example:
```
{
  colName: "abc",
  mapPathList: [
    "abc",
    ...
  ],
}
```

## `class com.dremio.dac.explore.model.extract.ReplaceCards`
- Example:
```
{
  cards: [
    {
      description: "abc",
      examplesList: [
        {
          positionList: [
            {
              length: 1,
              offset: 1,
            },
            ...
          ],
          text: "abc",
        },
        ...
      ],
      matchedCount: 1,
      rule: {
        ignoreCase: true | false,
        selectionPattern: "abc",
        selectionType: "CONTAINS" | "STARTS_WITH" | "ENDS_WITH" | "EXACT" | "MATCHES" | "IS_NULL",
      },
      unmatchedCount: 1,
    },
    ...
  ],
  values: {
    availableValues: [
      {
        count: 1,
        percent: 1.0,
        type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
        value: "abc",
        valueRange: {
          lowerLimit: any,
          upperLimit: any,
        },
      },
      ...
    ],
    availableValuesCount: 1,
    matchedValues: 1,
    unmatchedValues: 1,
  },
}
```

## `class com.dremio.dac.explore.model.extract.ReplaceCards$ReplaceValuesCard`
- Example:
```
{
  availableValues: [
    {
      count: 1,
      percent: 1.0,
      type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
      value: "abc",
      valueRange: {
        lowerLimit: any,
        upperLimit: any,
      },
    },
    ...
  ],
  availableValuesCount: 1,
  matchedValues: 1,
  unmatchedValues: 1,
}
```

## `class com.dremio.dac.explore.model.extract.Selection`
- Example:
```
{
  cellText: "abc",
  colName: "abc",
  length: 1,
  offset: 1,
}
```

## `class com.dremio.dac.model.folder.Folder`
- Example:
```
{ /** Folder **/
  contents: {
    canTagsBeSkipped: true | false,
    datasets: [
      {
        datasetConfig: {
          accelerated: true | false,
          calciteFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          contextList: [
            "abc",
            ...
          ],
          createdAt: 1,
          derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
          fieldOriginsList: [
            {
              name: "abc",
              originsList: [
                {
                  columnName: "abc",
                  derived: true | false,
                  tableList: [
                    "abc",
                    ...
                  ],
                },
                ...
              ],
            },
            ...
          ],
          fullPathList: [
            "abc",
            ...
          ],
          grandParentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          id: "abc",
          isNamed: true | false,
          lastTransform: any,
          name: "abc",
          owner: "abc",
          parentPath: "abc",
          parentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          previousVersion: {
            datasetPath: "abc",
            datasetVersion: "abc",
          },
          recordSchema: { /** ByteString **/
            empty: true | false,
          },
          savedTag: "abc",
          sql: "abc",
          sqlFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          state: {
            columnsList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            contextList: [
              "abc",
              ...
            ],
            filtersList: [
              {
                exclude: true | false,
                filterDef: any,
                keepNull: true | false,
                operand: any,
              },
              ...
            ],
            from: any,
            groupBysList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            joinsList: [
              {
                joinAlias: "abc",
                joinConditionsList: [
                  {
                    leftColumn: "abc",
                    rightColumn: "abc",
                  },
                  ...
                ],
                joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                rightTable: "abc",
              },
              ...
            ],
            ordersList: [
              {
                direction: "ASC" | "DESC",
                name: "abc",
              },
              ...
            ],
            referredTablesList: [
              "abc",
              ...
            ],
          },
          version: "abc",
        },
        datasetName: "abc",
        id: "abc",
        jobCount: 1,
        lastHistoryItem: {
          bytes: 1,
          createdAt: 1,
          datasetVersion: "abc",
          finishedAt: 1,
          owner: "abc",
          preview: true | false,
          recordsReturned: 1,
          state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
          transformDescription: "abc",
          versionedResourcePath: "abc",
        },
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        sql: "abc",
        tags: [
          "abc",
          ...
        ],
        versionedResourcePath: "abc",
      },
      ...
    ],
    files: [
      {
        fileFormat: {
          fileFormat: {
            ctime: 1,
            fullPath: [
              "abc",
              ...
            ],
            isFolder: true | false,
            location: "abc",
            name: "abc",
            owner: "abc",
            version: "abc",
          },
          id: "abc",
          links: {
            abc: "abc", ...
          },
        },
        filePath: "abc",
        id: "abc",
        isHomeFile: true | false,
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
      },
      ...
    ],
    folders: [
      (ref: Folder),
      ...
    ],
    physicalDatasets: [
      {
        datasetConfig: {
          formatSettings: {
            ctime: 1,
            extendedConfig: { /** ByteString **/
              empty: true | false,
            },
            fullPathList: [
              "abc",
              ...
            ],
            location: "abc",
            name: "abc",
            owner: "abc",
            tag: "abc",
            type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
            version: 1,
          },
          fullPathList: [
            "abc",
            ...
          ],
          id: "abc",
          name: "abc",
          tag: "abc",
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
          version: 1,
        },
        datasetName: "abc",
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        tags: [
          "abc",
          ...
        ],
      },
      ...
    ],
  },
  extendedConfig: {
    datasetCount: 1,
    descendants: 1,
    jobCount: 1,
  },
  fileSystemFolder: true | false,
  fullPathList: [
    "abc",
    ...
  ],
  id: "abc",
  isPhysicalDataset: true | false,
  links: {
    abc: "abc", ...
  },
  name: "abc",
  queryable: true | false,
  tags: [
    "abc",
    ...
  ],
  urlPath: "abc",
  version: "abc",
}
```

## `class com.dremio.dac.model.folder.FolderName`
- Example:
```
"abc"
```

## `class com.dremio.dac.model.job.JobDetailsUI`
- Example:
```
{
  accelerated: true | false,
  acceleration: {
    reflectionRelationships: [
      {
        accelerationSettings: {
          gracePeriod: 1,
          method: "FULL" | "INCREMENTAL",
          refreshField: "abc",
          refreshPeriod: 1,
        },
        dataset: {
          id: "abc",
          path: [
            "abc",
            ...
          ],
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
        },
        materialization: {
          id: "abc",
          refreshChainStartTime: 1,
        },
        reflection: {
          currentByteSize: 1,
          details: {
            dimensionFieldList: [
              {
                granularity: "DATE" | "NORMAL",
                name: "abc",
              },
              ...
            ],
            displayFieldList: [
              { /** LayoutFieldApiDescriptor **/
                name: "abc",
              },
              ...
            ],
            distributionFieldList: [
              { /** LayoutFieldApiDescriptor **/
                name: "abc",
              },
              ...
            ],
            measureFieldList: [
              {
                measureTypeList: [
                  "UNKNOWN" | "MIN" | "MAX" | "SUM" | "COUNT" | "APPROX_COUNT_DISTINCT",
                  ...
                ],
                name: "abc",
              },
              ...
            ],
            partitionDistributionStrategy: "CONSOLIDATED" | "STRIPED",
            partitionFieldList: [
              { /** LayoutFieldApiDescriptor **/
                name: "abc",
              },
              ...
            ],
            sortFieldList: [
              { /** LayoutFieldApiDescriptor **/
                name: "abc",
              },
              ...
            ],
          },
          error: {
            code: "PIPELINE_FAILURE" | "MATERIALIZATION_FAILURE" | "OTHER",
            materializationFailure: {
              jobId: "abc",
              materializationId: "abc",
            },
            message: "abc",
            stackTrace: "abc",
          },
          hasValidMaterialization: true | false,
          id: "abc",
          latestMaterializationState: "NEW" | "RUNNING" | "DONE" | "FAILED" | "DELETED",
          name: "abc",
          state: "ACTIVE" | "FAILED",
          totalByteSize: 1,
          type: "RAW" | "AGGREGATION" | "EXTERNAL",
        },
        relationship: "CONSIDERED" | "MATCHED" | "CHOSEN",
        snowflake: true | false,
      },
      ...
    ],
  },
  attemptDetails: [
    {
      commandPoolWaitTime: 1,
      engineStartTime: 1,
      executionPlanningTime: 1,
      executionTime: 1,
      metadataRetrievalTime: 1,
      pendingTime: 1,
      planningTime: 1,
      profileUrl: "abc",
      queuedTime: 1,
      reason: "abc",
      result: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
      runningTime: 1,
      startingTime: 1,
      totalTime: 1,
    },
    ...
  ],
  attemptsSummary: "abc",
  cancellationInfo: {
    message: "abc",
  },
  dataVolume: 1,
  datasetPathList: [
    "abc",
    ...
  ],
  datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
  datasetVersion: "abc",
  description: "abc",
  downloadUrl: "abc",
  endTime: 1,
  failureInfo: {
    errors: [
      {
        message: "abc",
        range: {
          endColumn: 1,
          endLine: 1,
          startColumn: 1,
          startLine: 1,
        },
      },
      ...
    ],
    message: "abc",
    type: "UNKNOWN" | "PARSE" | "VALIDATION" | "EXECUTION",
  },
  fsDatasetProfiles: [
    {
      dataVolumeInBytes: 1,
      datasetProfile: { /** CommonDatasetProfile **/
        bytesRead: 1,
        datasetPathsList: [
          {
            datasetPathList: [
              "abc",
              ...
            ],
          },
          ...
        ],
        locality: 1.0,
        parallelism: 1,
        recordsRead: 1,
        waitOnSource: 1,
      },
      percentageDataPruned: 1,
      prunedPathsList: [
        "abc",
        ...
      ],
    },
    ...
  ],
  jobId: {
    id: "abc",
    name: "abc",
  },
  materializationFor: {
    datasetId: "abc",
    layoutVersion: "abc",
    legacyLayoutVersion: 1,
    materializationId: "abc",
    reflectionId: "abc",
    reflectionName: "abc",
    reflectionType: "abc",
  },
  outputRecords: 1,
  paginationUrl: "abc",
  parentsList: [
    {
      datasetPathList: [
        "abc",
        ...
      ],
      type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    },
    ...
  ],
  peakMemory: 1,
  plansConsidered: 1,
  queryType: "UI_RUN" | "UI_PREVIEW" | "UI_INTERNAL_PREVIEW" | "UI_INTERNAL_RUN" | "UI_EXPORT" | "ODBC" | "JDBC" | "REST" | "ACCELERATOR_CREATE" | "ACCELERATOR_DROP" | "UNKNOWN" | "PREPARE_INTERNAL" | "ACCELERATOR_EXPLAIN" | "UI_INITIAL_PREVIEW" | "FLIGHT",
  requestType: "GET_CATALOGS" | "GET_COLUMNS" | "GET_SCHEMAS" | "GET_TABLES" | "CREATE_PREPARE" | "EXECUTE_PREPARE" | "RUN_SQL" | "GET_SERVER_META",
  resourceScheduling: {
    queueId: "abc",
    queueName: "abc",
    ruleContent: "abc",
    ruleId: "abc",
    ruleName: "abc",
  },
  resultsAvailable: true | false,
  snowflakeAccelerated: true | false,
  spillDetails: {
    hashAggSpilled: true | false,
    sortSpilled: true | false,
    totalBytesSpilled: 1,
  },
  spilled: true | false,
  sql: "abc",
  startTime: 1,
  state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
  stats: {
    inputBytes: 1,
    inputRecords: 1,
    isOutputLimited: true | false,
    outputBytes: 1,
    outputRecords: 1,
  },
  tableDatasetProfiles: [
    {
      datasetProfile: { /** CommonDatasetProfile **/
        bytesRead: 1,
        datasetPathsList: [
          {
            datasetPathList: [
              "abc",
              ...
            ],
          },
          ...
        ],
        locality: 1.0,
        parallelism: 1,
        recordsRead: 1,
        waitOnSource: 1,
      },
      pushdownQuery: "abc",
    },
    ...
  ],
  timeSpentInPlanning: 1,
  topOperations: [
    {
      timeConsumed: 1.0,
      type: "Client" | "Join" | "Aggregate" | "Filter" | "Project" | "Data_exchange" | "Reading" | "Writing" | "Sort" | "Union" | "Window" | "Limit" | "Complext_to_JSON" | "Producer_consumer" | "Flatten" | "Misc",
    },
    ...
  ],
  user: "abc",
  waitInClient: 1,
}
```

## `class com.dremio.dac.model.job.JobFilterItems`
- Example:
```
{
  items: [
    {
      id: "abc",
      label: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.model.job.JobUI`
- Example:
```
{
  jobAttempt: {
    attemptId: "abc",
    details: {
      dataVolume: 1,
      fsDatasetProfilesList: [
        {
          dataVolumeInBytes: 1,
          datasetProfile: { /** CommonDatasetProfile **/
            bytesRead: 1,
            datasetPathsList: [
              {
                datasetPathList: [
                  "abc",
                  ...
                ],
              },
              ...
            ],
            locality: 1.0,
            parallelism: 1,
            recordsRead: 1,
            waitOnSource: 1,
          },
          percentageDataPruned: 1,
          prunedPathsList: [
            "abc",
            ...
          ],
        },
        ...
      ],
      outputRecords: 1,
      peakMemory: 1,
      plansConsidered: 1,
      tableDatasetProfilesList: [
        {
          datasetProfile: { /** CommonDatasetProfile **/
            bytesRead: 1,
            datasetPathsList: [
              {
                datasetPathList: [
                  "abc",
                  ...
                ],
              },
              ...
            ],
            locality: 1.0,
            parallelism: 1,
            recordsRead: 1,
            waitOnSource: 1,
          },
          pushdownQuery: "abc",
        },
        ...
      ],
      timeSpentInPlanning: 1,
      topOperationsList: [
        {
          timeConsumed: 1.0,
          type: "Client" | "Join" | "Aggregate" | "Filter" | "Project" | "Data_exchange" | "Reading" | "Writing" | "Sort" | "Union" | "Window" | "Limit" | "Complext_to_JSON" | "Producer_consumer" | "Flatten" | "Misc",
        },
        ...
      ],
      waitInClient: 1,
    },
    endpoint: { /** NodeEndpoint **/
      address: "abc",
      availableCores: 1,
      conduitPort: 1,
      dremioVersion: "abc",
      engineId: {
        id: "abc",
      },
      fabricPort: 1,
      maxDirectMemory: 1,
      nodeTag: "abc",
      provisionId: "abc",
      roles: {
        distributedCache: true | false,
        javaExecutor: true | false,
        logicalPlan: true | false,
        master: true | false,
        physicalPlan: true | false,
        sqlQuery: true | false,
      },
      startTime: 1,
      subEngineId: {
        id: "abc",
      },
      userPort: 1,
    },
    info: {
      acceleration: {
        acceleratedCost: 1.0,
        substitutionsList: [
          {
            id: {
              accelerationId: "abc",
              layoutId: "abc",
              materializationId: "abc",
            },
            originalCost: 1.0,
            speedup: 1.0,
            tablePathList: [
              "abc",
              ...
            ],
          },
          ...
        ],
      },
      appId: "abc",
      cancellationInfo: {
        message: "abc",
      },
      client: "abc",
      contextList: [
        "abc",
        ...
      ],
      datasetPathList: [
        "abc",
        ...
      ],
      datasetVersion: "abc",
      description: "abc",
      detailedFailureInfo: {
        errorsList: [
          {
            endColumn: 1,
            endLine: 1,
            message: "abc",
            startColumn: 1,
            startLine: 1,
          },
          ...
        ],
        message: "abc",
        type: "UNKNOWN" | "PARSE" | "PLAN" | "VALIDATION" | "EXECUTION",
      },
      downloadInfo: {
        downloadId: "abc",
        fileName: "abc",
      },
      failureInfo: "abc",
      fieldOriginsList: [
        {
          name: "abc",
          originsList: [
            {
              columnName: "abc",
              derived: true | false,
              tableList: [
                "abc",
                ...
              ],
            },
            ...
          ],
        },
        ...
      ],
      finishTime: 1,
      grandParentsList: [
        {
          datasetPathList: [
            "abc",
            ...
          ],
          level: 1,
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
        },
        ...
      ],
      jobId: { /** JobId **/
        id: "abc",
        name: "abc",
      },
      joinAnalysis: {
        joinStatsList: [
          {
            buildInputCount: 1,
            joinConditionsList: [
              {
                buildSideColumn: "abc",
                buildSideTableId: 1,
                probeSideColumn: "abc",
                probeSideTableId: 1,
              },
              ...
            ],
            joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
            outputRecords: 1,
            probeInputCount: 1,
            unmatchedBuildCount: 1,
            unmatchedProbeCount: 1,
          },
          ...
        ],
        joinTablesList: [
          {
            tableId: 1,
            tableSchemaPathList: [
              "abc",
              ...
            ],
          },
          ...
        ],
      },
      joinsList: [
        {
          conditionsList: [
            {
              columnA: "abc",
              columnB: "abc",
              tableAList: [
                "abc",
                ...
              ],
              tableBList: [
                "abc",
                ...
              ],
            },
            ...
          ],
          degreesOfSeparation: 1,
          joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
          leftTablePathList: [
            "abc",
            ...
          ],
          rightTablePathList: [
            "abc",
            ...
          ],
        },
        ...
      ],
      materializationFor: {
        datasetId: "abc",
        layoutVersion: "abc",
        legacyLayoutVersion: 1,
        materializationId: "abc",
        reflectionId: "abc",
        reflectionName: "abc",
        reflectionType: "abc",
      },
      originalCost: 1.0,
      outputTableList: [
        "abc",
        ...
      ],
      parentsList: [
        {
          datasetPathList: [
            "abc",
            ...
          ],
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
        },
        ...
      ],
      partitionsList: [
        "abc",
        ...
      ],
      queryType: "UI_RUN" | "UI_PREVIEW" | "UI_INTERNAL_PREVIEW" | "UI_INTERNAL_RUN" | "UI_EXPORT" | "ODBC" | "JDBC" | "REST" | "ACCELERATOR_CREATE" | "ACCELERATOR_DROP" | "UNKNOWN" | "PREPARE_INTERNAL" | "ACCELERATOR_EXPLAIN" | "UI_INITIAL_PREVIEW" | "FLIGHT",
      requestType: "GET_CATALOGS" | "GET_COLUMNS" | "GET_SCHEMAS" | "GET_TABLES" | "CREATE_PREPARE" | "EXECUTE_PREPARE" | "RUN_SQL" | "GET_SERVER_META",
      resourceSchedulingInfo: {
        queryCost: 1.0,
        queueId: "abc",
        queueName: "abc",
        resourceSchedulingEnd: 1,
        resourceSchedulingStart: 1,
        ruleContent: "abc",
        ruleId: "abc",
        ruleName: "abc",
      },
      resultMetadataList: [
        {
          footer: {
            batchList: [
              {
                offset: 1,
                recordCount: 1,
              },
              ...
            ],
            fieldList: [
              { /** SerializedField **/
                bufferLength: 1,
                childList: [
                  (ref: SerializedField),
                  ...
                ],
                majorType: {
                  minorType: "LATE" | "STRUCT" | "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "DECIMAL9" | "DECIMAL18" | "DECIMAL28SPARSE" | "DECIMAL38SPARSE" | "MONEY" | "DATE" | "TIME" | "TIMETZ" | "TIMESTAMPTZ" | "TIMESTAMP" | "INTERVAL" | "FLOAT4" | "FLOAT8" | "BIT" | "FIXEDCHAR" | "FIXED16CHAR" | "FIXEDSIZEBINARY" | "VARCHAR" | "VAR16CHAR" | "VARBINARY" | "UINT1" | "UINT2" | "UINT4" | "UINT8" | "DECIMAL28DENSE" | "DECIMAL38DENSE" | "NULL" | "INTERVALYEAR" | "INTERVALDAY" | "LIST" | "GENERIC_OBJECT" | "UNION" | "DECIMAL",
                  mode: "OPTIONAL" | "REQUIRED" | "REPEATED",
                  precision: 1,
                  scale: 1,
                  subTypeList: [
                    "LATE" | "STRUCT" | "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "DECIMAL9" | "DECIMAL18" | "DECIMAL28SPARSE" | "DECIMAL38SPARSE" | "MONEY" | "DATE" | "TIME" | "TIMETZ" | "TIMESTAMPTZ" | "TIMESTAMP" | "INTERVAL" | "FLOAT4" | "FLOAT8" | "BIT" | "FIXEDCHAR" | "FIXED16CHAR" | "FIXEDSIZEBINARY" | "VARCHAR" | "VAR16CHAR" | "VARBINARY" | "UINT1" | "UINT2" | "UINT4" | "UINT8" | "DECIMAL28DENSE" | "DECIMAL38DENSE" | "NULL" | "INTERVALYEAR" | "INTERVALDAY" | "LIST" | "GENERIC_OBJECT" | "UNION" | "DECIMAL",
                    ...
                  ],
                  timeZone: 1,
                  width: 1,
                },
                namePart: { /** NamePart **/
                  child: (ref: NamePart),
                  name: "abc",
                  type: "NAME" | "ARRAY",
                },
                valueCount: 1,
                varByteLength: 1,
              },
              ...
            ],
          },
          path: "abc",
          recordCount: 1,
          screenNodeEndpoint: { /** NodeEndpoint **/
            address: "abc",
            availableCores: 1,
            conduitPort: 1,
            dremioVersion: "abc",
            engineId: {
              id: "abc",
            },
            fabricPort: 1,
            maxDirectMemory: 1,
            nodeTag: "abc",
            provisionId: "abc",
            roles: {
              distributedCache: true | false,
              javaExecutor: true | false,
              logicalPlan: true | false,
              master: true | false,
              physicalPlan: true | false,
              sqlQuery: true | false,
            },
            startTime: 1,
            subEngineId: {
              id: "abc",
            },
            userPort: 1,
          },
        },
        ...
      ],
      scanPathsList: [
        {
          pathList: [
            "abc",
            ...
          ],
        },
        ...
      ],
      space: "abc",
      spillJobDetails: {
        totalBytesSpilledByHashAgg: 1,
        totalBytesSpilledBySort: 1,
      },
      sql: "abc",
      startTime: 1,
      user: "abc",
    },
    reason: "NONE" | "OUT_OF_MEMORY" | "SCHEMA_CHANGE" | "INVALID_DATASET_METADATA" | "JSON_FIELD_CHANGE" | "RESOURCE_TIMEOUT",
    state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
    stats: {
      inputBytes: 1,
      inputRecords: 1,
      isOutputLimited: true | false,
      outputBytes: 1,
      outputRecords: 1,
    },
  },
  jobId: { /** JobId **/
    id: "abc",
    name: "abc",
  },
}
```

## `class com.dremio.dac.model.job.JobsUI`
- Example:
```
{
  jobs: [
    {
      accelerated: true | false,
      cancellationInfo: {
        message: "abc",
      },
      datasetPathList: [
        "abc",
        ...
      ],
      datasetType: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
      datasetVersion: "abc",
      description: "abc",
      endTime: 1,
      failureInfo: {
        errors: [
          {
            message: "abc",
            range: {
              endColumn: 1,
              endLine: 1,
              startColumn: 1,
              startLine: 1,
            },
          },
          ...
        ],
        message: "abc",
        type: "UNKNOWN" | "PARSE" | "VALIDATION" | "EXECUTION",
      },
      id: "abc",
      isComplete: true | false,
      outputLimited: true | false,
      outputRecords: 1,
      requestType: "GET_CATALOGS" | "GET_COLUMNS" | "GET_SCHEMAS" | "GET_TABLES" | "CREATE_PREPARE" | "EXECUTE_PREPARE" | "RUN_SQL" | "GET_SERVER_META",
      snowflakeAccelerated: true | false,
      spilled: true | false,
      startTime: 1,
      state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
      user: "abc",
    },
    ...
  ],
  next: "abc",
}
```

## `class com.dremio.dac.model.resourcetree.ResourceList`
- Example:
```
{
  resources: [
    { /** ResourceTreeEntity **/
      fullPath: [
        "abc",
        ...
      ],
      name: "abc",
      resources: [
        (ref: ResourceTreeEntity),
        ...
      ],
      type: "SOURCE" | "SPACE" | "FOLDER" | "HOME" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
      url: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.model.sources.PhysicalDataset`
- Example:
```
{
  datasetConfig: {
    formatSettings: {
      ctime: 1,
      extendedConfig: {
        empty: true | false,
      },
      fullPathList: [
        "abc",
        ...
      ],
      location: "abc",
      name: "abc",
      owner: "abc",
      tag: "abc",
      type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
      version: 1,
    },
    fullPathList: [
      "abc",
      ...
    ],
    id: "abc",
    name: "abc",
    tag: "abc",
    type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
    version: 1,
  },
  datasetName: "abc",
  jobCount: 1,
  links: {
    abc: "abc", ...
  },
  resourcePath: "abc",
  tags: [
    "abc",
    ...
  ],
}
```

## `class com.dremio.dac.model.sources.SourceUI`
- Example:
```
{
  accelerationGracePeriod: 1,
  accelerationNeverExpire: true | false,
  accelerationNeverRefresh: true | false,
  accelerationRefreshPeriod: 1,
  allowCrossSourceSelection: true | false,
  config: {
  },
  contents: { /** NamespaceTree **/
    canTagsBeSkipped: true | false,
    datasets: [
      {
        datasetConfig: {
          accelerated: true | false,
          calciteFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          contextList: [
            "abc",
            ...
          ],
          createdAt: 1,
          derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
          fieldOriginsList: [
            {
              name: "abc",
              originsList: [
                {
                  columnName: "abc",
                  derived: true | false,
                  tableList: [
                    "abc",
                    ...
                  ],
                },
                ...
              ],
            },
            ...
          ],
          fullPathList: [
            "abc",
            ...
          ],
          grandParentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          id: "abc",
          isNamed: true | false,
          lastTransform: any,
          name: "abc",
          owner: "abc",
          parentPath: "abc",
          parentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          previousVersion: {
            datasetPath: "abc",
            datasetVersion: "abc",
          },
          recordSchema: { /** ByteString **/
            empty: true | false,
          },
          savedTag: "abc",
          sql: "abc",
          sqlFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          state: {
            columnsList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            contextList: [
              "abc",
              ...
            ],
            filtersList: [
              {
                exclude: true | false,
                filterDef: any,
                keepNull: true | false,
                operand: any,
              },
              ...
            ],
            from: any,
            groupBysList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            joinsList: [
              {
                joinAlias: "abc",
                joinConditionsList: [
                  {
                    leftColumn: "abc",
                    rightColumn: "abc",
                  },
                  ...
                ],
                joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                rightTable: "abc",
              },
              ...
            ],
            ordersList: [
              {
                direction: "ASC" | "DESC",
                name: "abc",
              },
              ...
            ],
            referredTablesList: [
              "abc",
              ...
            ],
          },
          version: "abc",
        },
        datasetName: "abc",
        id: "abc",
        jobCount: 1,
        lastHistoryItem: {
          bytes: 1,
          createdAt: 1,
          datasetVersion: "abc",
          finishedAt: 1,
          owner: "abc",
          preview: true | false,
          recordsReturned: 1,
          state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
          transformDescription: "abc",
          versionedResourcePath: "abc",
        },
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        sql: "abc",
        tags: [
          "abc",
          ...
        ],
        versionedResourcePath: "abc",
      },
      ...
    ],
    files: [
      {
        fileFormat: {
          fileFormat: {
            ctime: 1,
            fullPath: [
              "abc",
              ...
            ],
            isFolder: true | false,
            location: "abc",
            name: "abc",
            owner: "abc",
            version: "abc",
          },
          id: "abc",
          links: {
            abc: "abc", ...
          },
        },
        filePath: "abc",
        id: "abc",
        isHomeFile: true | false,
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
      },
      ...
    ],
    folders: [
      {
        contents: (ref: NamespaceTree),
        extendedConfig: {
          datasetCount: 1,
          descendants: 1,
          jobCount: 1,
        },
        fileSystemFolder: true | false,
        fullPathList: [
          "abc",
          ...
        ],
        id: "abc",
        isPhysicalDataset: true | false,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
        version: "abc",
      },
      ...
    ],
    physicalDatasets: [
      {
        datasetConfig: {
          formatSettings: {
            ctime: 1,
            extendedConfig: { /** ByteString **/
              empty: true | false,
            },
            fullPathList: [
              "abc",
              ...
            ],
            location: "abc",
            name: "abc",
            owner: "abc",
            tag: "abc",
            type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
            version: 1,
          },
          fullPathList: [
            "abc",
            ...
          ],
          id: "abc",
          name: "abc",
          tag: "abc",
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
          version: 1,
        },
        datasetName: "abc",
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        tags: [
          "abc",
          ...
        ],
      },
      ...
    ],
  },
  ctime: 1,
  datasetCountBounded: true | false,
  description: "abc",
  fullPathList: [
    "abc",
    ...
  ],
  id: "abc",
  img: "abc",
  links: {
    abc: "abc", ...
  },
  metadataPolicy: {
    authTTLMillis: 1,
    autoPromoteDatasets: true | false,
    datasetDefinitionExpireAfterMillis: 1,
    datasetDefinitionRefreshAfterMillis: 1,
    deleteUnavailableDatasets: true | false,
    namesRefreshMillis: 1,
    updateMode: "PREFETCH" | "PREFETCH_QUERIED" | "INLINE",
  },
  name: "abc",
  namespaceAttributes: [
    any,
    ...
  ],
  numberOfDatasets: 1,
  resourcePath: "abc",
  state: {
    messages: [
      {
        level: "INFO" | "WARN" | "ERROR",
        message: "abc",
      },
      ...
    ],
    status: "good" | "bad" | "warn",
    suggestedUserAction: "abc",
  },
  tag: "abc",
}
```

## `class com.dremio.dac.model.sources.Sources`
- Example:
```
{
  sources: [
    {
      accelerationGracePeriod: 1,
      accelerationNeverExpire: true | false,
      accelerationNeverRefresh: true | false,
      accelerationRefreshPeriod: 1,
      allowCrossSourceSelection: true | false,
      config: {
      },
      contents: { /** NamespaceTree **/
        canTagsBeSkipped: true | false,
        datasets: [
          {
            datasetConfig: {
              accelerated: true | false,
              calciteFieldsList: [
                { /** ViewFieldType **/
                  endUnit: "abc",
                  fractionalSecondPrecision: 1,
                  isNullable: true | false,
                  name: "abc",
                  precision: 1,
                  scale: 1,
                  startUnit: "abc",
                  type: "abc",
                  typeFamily: "abc",
                },
                ...
              ],
              contextList: [
                "abc",
                ...
              ],
              createdAt: 1,
              derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
              fieldOriginsList: [
                {
                  name: "abc",
                  originsList: [
                    {
                      columnName: "abc",
                      derived: true | false,
                      tableList: [
                        "abc",
                        ...
                      ],
                    },
                    ...
                  ],
                },
                ...
              ],
              fullPathList: [
                "abc",
                ...
              ],
              grandParentsList: [
                { /** ParentDataset **/
                  datasetPathList: [
                    "abc",
                    ...
                  ],
                  level: 1,
                  type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
                },
                ...
              ],
              id: "abc",
              isNamed: true | false,
              lastTransform: any,
              name: "abc",
              owner: "abc",
              parentPath: "abc",
              parentsList: [
                { /** ParentDataset **/
                  datasetPathList: [
                    "abc",
                    ...
                  ],
                  level: 1,
                  type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
                },
                ...
              ],
              previousVersion: {
                datasetPath: "abc",
                datasetVersion: "abc",
              },
              recordSchema: { /** ByteString **/
                empty: true | false,
              },
              savedTag: "abc",
              sql: "abc",
              sqlFieldsList: [
                { /** ViewFieldType **/
                  endUnit: "abc",
                  fractionalSecondPrecision: 1,
                  isNullable: true | false,
                  name: "abc",
                  precision: 1,
                  scale: 1,
                  startUnit: "abc",
                  type: "abc",
                  typeFamily: "abc",
                },
                ...
              ],
              state: {
                columnsList: [
                  { /** Column **/
                    name: "abc",
                    value: any,
                  },
                  ...
                ],
                contextList: [
                  "abc",
                  ...
                ],
                filtersList: [
                  {
                    exclude: true | false,
                    filterDef: any,
                    keepNull: true | false,
                    operand: any,
                  },
                  ...
                ],
                from: any,
                groupBysList: [
                  { /** Column **/
                    name: "abc",
                    value: any,
                  },
                  ...
                ],
                joinsList: [
                  {
                    joinAlias: "abc",
                    joinConditionsList: [
                      {
                        leftColumn: "abc",
                        rightColumn: "abc",
                      },
                      ...
                    ],
                    joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                    rightTable: "abc",
                  },
                  ...
                ],
                ordersList: [
                  {
                    direction: "ASC" | "DESC",
                    name: "abc",
                  },
                  ...
                ],
                referredTablesList: [
                  "abc",
                  ...
                ],
              },
              version: "abc",
            },
            datasetName: "abc",
            id: "abc",
            jobCount: 1,
            lastHistoryItem: {
              bytes: 1,
              createdAt: 1,
              datasetVersion: "abc",
              finishedAt: 1,
              owner: "abc",
              preview: true | false,
              recordsReturned: 1,
              state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
              transformDescription: "abc",
              versionedResourcePath: "abc",
            },
            links: {
              abc: "abc", ...
            },
            resourcePath: "abc",
            sql: "abc",
            tags: [
              "abc",
              ...
            ],
            versionedResourcePath: "abc",
          },
          ...
        ],
        files: [
          {
            fileFormat: {
              fileFormat: {
                ctime: 1,
                fullPath: [
                  "abc",
                  ...
                ],
                isFolder: true | false,
                location: "abc",
                name: "abc",
                owner: "abc",
                version: "abc",
              },
              id: "abc",
              links: {
                abc: "abc", ...
              },
            },
            filePath: "abc",
            id: "abc",
            isHomeFile: true | false,
            jobCount: 1,
            links: {
              abc: "abc", ...
            },
            name: "abc",
            queryable: true | false,
            tags: [
              "abc",
              ...
            ],
            urlPath: "abc",
          },
          ...
        ],
        folders: [
          {
            contents: (ref: NamespaceTree),
            extendedConfig: {
              datasetCount: 1,
              descendants: 1,
              jobCount: 1,
            },
            fileSystemFolder: true | false,
            fullPathList: [
              "abc",
              ...
            ],
            id: "abc",
            isPhysicalDataset: true | false,
            links: {
              abc: "abc", ...
            },
            name: "abc",
            queryable: true | false,
            tags: [
              "abc",
              ...
            ],
            urlPath: "abc",
            version: "abc",
          },
          ...
        ],
        physicalDatasets: [
          {
            datasetConfig: {
              formatSettings: {
                ctime: 1,
                extendedConfig: { /** ByteString **/
                  empty: true | false,
                },
                fullPathList: [
                  "abc",
                  ...
                ],
                location: "abc",
                name: "abc",
                owner: "abc",
                tag: "abc",
                type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
                version: 1,
              },
              fullPathList: [
                "abc",
                ...
              ],
              id: "abc",
              name: "abc",
              tag: "abc",
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
              version: 1,
            },
            datasetName: "abc",
            jobCount: 1,
            links: {
              abc: "abc", ...
            },
            resourcePath: "abc",
            tags: [
              "abc",
              ...
            ],
          },
          ...
        ],
      },
      ctime: 1,
      datasetCountBounded: true | false,
      description: "abc",
      fullPathList: [
        "abc",
        ...
      ],
      id: "abc",
      img: "abc",
      links: {
        abc: "abc", ...
      },
      metadataPolicy: {
        authTTLMillis: 1,
        autoPromoteDatasets: true | false,
        datasetDefinitionExpireAfterMillis: 1,
        datasetDefinitionRefreshAfterMillis: 1,
        deleteUnavailableDatasets: true | false,
        namesRefreshMillis: 1,
        updateMode: "PREFETCH" | "PREFETCH_QUERIED" | "INLINE",
      },
      name: "abc",
      namespaceAttributes: [
        any,
        ...
      ],
      numberOfDatasets: 1,
      resourcePath: "abc",
      state: {
        messages: [
          {
            level: "INFO" | "WARN" | "ERROR",
            message: "abc",
          },
          ...
        ],
        status: "good" | "bad" | "warn",
        suggestedUserAction: "abc",
      },
      tag: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.model.spaces.Home`
- Example:
```
{
  contents: { /** NamespaceTree **/
    canTagsBeSkipped: true | false,
    datasets: [
      {
        datasetConfig: {
          accelerated: true | false,
          calciteFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          contextList: [
            "abc",
            ...
          ],
          createdAt: 1,
          derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
          fieldOriginsList: [
            {
              name: "abc",
              originsList: [
                {
                  columnName: "abc",
                  derived: true | false,
                  tableList: [
                    "abc",
                    ...
                  ],
                },
                ...
              ],
            },
            ...
          ],
          fullPathList: [
            "abc",
            ...
          ],
          grandParentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          id: "abc",
          isNamed: true | false,
          lastTransform: any,
          name: "abc",
          owner: "abc",
          parentPath: "abc",
          parentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          previousVersion: {
            datasetPath: "abc",
            datasetVersion: "abc",
          },
          recordSchema: { /** ByteString **/
            empty: true | false,
          },
          savedTag: "abc",
          sql: "abc",
          sqlFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          state: {
            columnsList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            contextList: [
              "abc",
              ...
            ],
            filtersList: [
              {
                exclude: true | false,
                filterDef: any,
                keepNull: true | false,
                operand: any,
              },
              ...
            ],
            from: any,
            groupBysList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            joinsList: [
              {
                joinAlias: "abc",
                joinConditionsList: [
                  {
                    leftColumn: "abc",
                    rightColumn: "abc",
                  },
                  ...
                ],
                joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                rightTable: "abc",
              },
              ...
            ],
            ordersList: [
              {
                direction: "ASC" | "DESC",
                name: "abc",
              },
              ...
            ],
            referredTablesList: [
              "abc",
              ...
            ],
          },
          version: "abc",
        },
        datasetName: "abc",
        id: "abc",
        jobCount: 1,
        lastHistoryItem: {
          bytes: 1,
          createdAt: 1,
          datasetVersion: "abc",
          finishedAt: 1,
          owner: "abc",
          preview: true | false,
          recordsReturned: 1,
          state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
          transformDescription: "abc",
          versionedResourcePath: "abc",
        },
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        sql: "abc",
        tags: [
          "abc",
          ...
        ],
        versionedResourcePath: "abc",
      },
      ...
    ],
    files: [
      {
        fileFormat: {
          fileFormat: {
            ctime: 1,
            fullPath: [
              "abc",
              ...
            ],
            isFolder: true | false,
            location: "abc",
            name: "abc",
            owner: "abc",
            version: "abc",
          },
          id: "abc",
          links: {
            abc: "abc", ...
          },
        },
        filePath: "abc",
        id: "abc",
        isHomeFile: true | false,
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
      },
      ...
    ],
    folders: [
      {
        contents: (ref: NamespaceTree),
        extendedConfig: { /** ExtendedConfig **/
          datasetCount: 1,
          descendants: 1,
          jobCount: 1,
        },
        fileSystemFolder: true | false,
        fullPathList: [
          "abc",
          ...
        ],
        id: "abc",
        isPhysicalDataset: true | false,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
        version: "abc",
      },
      ...
    ],
    physicalDatasets: [
      {
        datasetConfig: {
          formatSettings: {
            ctime: 1,
            extendedConfig: { /** ByteString **/
              empty: true | false,
            },
            fullPathList: [
              "abc",
              ...
            ],
            location: "abc",
            name: "abc",
            owner: "abc",
            tag: "abc",
            type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
            version: 1,
          },
          fullPathList: [
            "abc",
            ...
          ],
          id: "abc",
          name: "abc",
          tag: "abc",
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
          version: 1,
        },
        datasetName: "abc",
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        tags: [
          "abc",
          ...
        ],
      },
      ...
    ],
  },
  ctime: 1,
  description: "abc",
  fullPathList: [
    "abc",
    ...
  ],
  homeConfig: {
    ctime: 1,
    extendedConfig: { /** ExtendedConfig **/
      datasetCount: 1,
      descendants: 1,
      jobCount: 1,
    },
    id: {
      id: "abc",
    },
    owner: "abc",
    tag: "abc",
    version: 1,
  },
  id: "abc",
  links: {
    abc: "abc", ...
  },
  name: "abc",
  owner: "abc",
}
```

## `class com.dremio.dac.model.spaces.Space`
- Example:
```
{
  contents: { /** NamespaceTree **/
    canTagsBeSkipped: true | false,
    datasets: [
      {
        datasetConfig: {
          accelerated: true | false,
          calciteFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          contextList: [
            "abc",
            ...
          ],
          createdAt: 1,
          derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
          fieldOriginsList: [
            {
              name: "abc",
              originsList: [
                {
                  columnName: "abc",
                  derived: true | false,
                  tableList: [
                    "abc",
                    ...
                  ],
                },
                ...
              ],
            },
            ...
          ],
          fullPathList: [
            "abc",
            ...
          ],
          grandParentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          id: "abc",
          isNamed: true | false,
          lastTransform: any,
          name: "abc",
          owner: "abc",
          parentPath: "abc",
          parentsList: [
            { /** ParentDataset **/
              datasetPathList: [
                "abc",
                ...
              ],
              level: 1,
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
            },
            ...
          ],
          previousVersion: {
            datasetPath: "abc",
            datasetVersion: "abc",
          },
          recordSchema: { /** ByteString **/
            empty: true | false,
          },
          savedTag: "abc",
          sql: "abc",
          sqlFieldsList: [
            { /** ViewFieldType **/
              endUnit: "abc",
              fractionalSecondPrecision: 1,
              isNullable: true | false,
              name: "abc",
              precision: 1,
              scale: 1,
              startUnit: "abc",
              type: "abc",
              typeFamily: "abc",
            },
            ...
          ],
          state: {
            columnsList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            contextList: [
              "abc",
              ...
            ],
            filtersList: [
              {
                exclude: true | false,
                filterDef: any,
                keepNull: true | false,
                operand: any,
              },
              ...
            ],
            from: any,
            groupBysList: [
              { /** Column **/
                name: "abc",
                value: any,
              },
              ...
            ],
            joinsList: [
              {
                joinAlias: "abc",
                joinConditionsList: [
                  {
                    leftColumn: "abc",
                    rightColumn: "abc",
                  },
                  ...
                ],
                joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                rightTable: "abc",
              },
              ...
            ],
            ordersList: [
              {
                direction: "ASC" | "DESC",
                name: "abc",
              },
              ...
            ],
            referredTablesList: [
              "abc",
              ...
            ],
          },
          version: "abc",
        },
        datasetName: "abc",
        id: "abc",
        jobCount: 1,
        lastHistoryItem: {
          bytes: 1,
          createdAt: 1,
          datasetVersion: "abc",
          finishedAt: 1,
          owner: "abc",
          preview: true | false,
          recordsReturned: 1,
          state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
          transformDescription: "abc",
          versionedResourcePath: "abc",
        },
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        sql: "abc",
        tags: [
          "abc",
          ...
        ],
        versionedResourcePath: "abc",
      },
      ...
    ],
    files: [
      {
        fileFormat: {
          fileFormat: {
            ctime: 1,
            fullPath: [
              "abc",
              ...
            ],
            isFolder: true | false,
            location: "abc",
            name: "abc",
            owner: "abc",
            version: "abc",
          },
          id: "abc",
          links: {
            abc: "abc", ...
          },
        },
        filePath: "abc",
        id: "abc",
        isHomeFile: true | false,
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
      },
      ...
    ],
    folders: [
      {
        contents: (ref: NamespaceTree),
        extendedConfig: {
          datasetCount: 1,
          descendants: 1,
          jobCount: 1,
        },
        fileSystemFolder: true | false,
        fullPathList: [
          "abc",
          ...
        ],
        id: "abc",
        isPhysicalDataset: true | false,
        links: {
          abc: "abc", ...
        },
        name: "abc",
        queryable: true | false,
        tags: [
          "abc",
          ...
        ],
        urlPath: "abc",
        version: "abc",
      },
      ...
    ],
    physicalDatasets: [
      {
        datasetConfig: {
          formatSettings: {
            ctime: 1,
            extendedConfig: { /** ByteString **/
              empty: true | false,
            },
            fullPathList: [
              "abc",
              ...
            ],
            location: "abc",
            name: "abc",
            owner: "abc",
            tag: "abc",
            type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
            version: 1,
          },
          fullPathList: [
            "abc",
            ...
          ],
          id: "abc",
          name: "abc",
          tag: "abc",
          type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
          version: 1,
        },
        datasetName: "abc",
        jobCount: 1,
        links: {
          abc: "abc", ...
        },
        resourcePath: "abc",
        tags: [
          "abc",
          ...
        ],
      },
      ...
    ],
  },
  ctime: 1,
  datasetCount: 1,
  description: "abc",
  fullPathList: [
    "abc",
    ...
  ],
  id: "abc",
  links: {
    abc: "abc", ...
  },
  name: "abc",
  version: "abc",
}
```

## `class com.dremio.dac.model.spaces.Spaces`
- Example:
```
{
  spaces: [
    {
      contents: { /** NamespaceTree **/
        canTagsBeSkipped: true | false,
        datasets: [
          {
            datasetConfig: {
              accelerated: true | false,
              calciteFieldsList: [
                { /** ViewFieldType **/
                  endUnit: "abc",
                  fractionalSecondPrecision: 1,
                  isNullable: true | false,
                  name: "abc",
                  precision: 1,
                  scale: 1,
                  startUnit: "abc",
                  type: "abc",
                  typeFamily: "abc",
                },
                ...
              ],
              contextList: [
                "abc",
                ...
              ],
              createdAt: 1,
              derivation: "SQL" | "DERIVED_UNKNOWN" | "DERIVED_PHYSICAL" | "DERIVED_VIRTUAL" | "UNKNOWN",
              fieldOriginsList: [
                {
                  name: "abc",
                  originsList: [
                    {
                      columnName: "abc",
                      derived: true | false,
                      tableList: [
                        "abc",
                        ...
                      ],
                    },
                    ...
                  ],
                },
                ...
              ],
              fullPathList: [
                "abc",
                ...
              ],
              grandParentsList: [
                { /** ParentDataset **/
                  datasetPathList: [
                    "abc",
                    ...
                  ],
                  level: 1,
                  type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
                },
                ...
              ],
              id: "abc",
              isNamed: true | false,
              lastTransform: any,
              name: "abc",
              owner: "abc",
              parentPath: "abc",
              parentsList: [
                { /** ParentDataset **/
                  datasetPathList: [
                    "abc",
                    ...
                  ],
                  level: 1,
                  type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
                },
                ...
              ],
              previousVersion: {
                datasetPath: "abc",
                datasetVersion: "abc",
              },
              recordSchema: { /** ByteString **/
                empty: true | false,
              },
              savedTag: "abc",
              sql: "abc",
              sqlFieldsList: [
                { /** ViewFieldType **/
                  endUnit: "abc",
                  fractionalSecondPrecision: 1,
                  isNullable: true | false,
                  name: "abc",
                  precision: 1,
                  scale: 1,
                  startUnit: "abc",
                  type: "abc",
                  typeFamily: "abc",
                },
                ...
              ],
              state: {
                columnsList: [
                  { /** Column **/
                    name: "abc",
                    value: any,
                  },
                  ...
                ],
                contextList: [
                  "abc",
                  ...
                ],
                filtersList: [
                  {
                    exclude: true | false,
                    filterDef: any,
                    keepNull: true | false,
                    operand: any,
                  },
                  ...
                ],
                from: any,
                groupBysList: [
                  { /** Column **/
                    name: "abc",
                    value: any,
                  },
                  ...
                ],
                joinsList: [
                  {
                    joinAlias: "abc",
                    joinConditionsList: [
                      {
                        leftColumn: "abc",
                        rightColumn: "abc",
                      },
                      ...
                    ],
                    joinType: "Inner" | "LeftOuter" | "RightOuter" | "FullOuter",
                    rightTable: "abc",
                  },
                  ...
                ],
                ordersList: [
                  {
                    direction: "ASC" | "DESC",
                    name: "abc",
                  },
                  ...
                ],
                referredTablesList: [
                  "abc",
                  ...
                ],
              },
              version: "abc",
            },
            datasetName: "abc",
            id: "abc",
            jobCount: 1,
            lastHistoryItem: {
              bytes: 1,
              createdAt: 1,
              datasetVersion: "abc",
              finishedAt: 1,
              owner: "abc",
              preview: true | false,
              recordsReturned: 1,
              state: "NOT_SUBMITTED" | "STARTING" | "RUNNING" | "COMPLETED" | "CANCELED" | "FAILED" | "CANCELLATION_REQUESTED" | "ENQUEUED" | "PLANNING" | "PENDING" | "METADATA_RETRIEVAL" | "QUEUED" | "ENGINE_START" | "EXECUTION_PLANNING" | "INVALID_STATE",
              transformDescription: "abc",
              versionedResourcePath: "abc",
            },
            links: {
              abc: "abc", ...
            },
            resourcePath: "abc",
            sql: "abc",
            tags: [
              "abc",
              ...
            ],
            versionedResourcePath: "abc",
          },
          ...
        ],
        files: [
          {
            fileFormat: {
              fileFormat: {
                ctime: 1,
                fullPath: [
                  "abc",
                  ...
                ],
                isFolder: true | false,
                location: "abc",
                name: "abc",
                owner: "abc",
                version: "abc",
              },
              id: "abc",
              links: {
                abc: "abc", ...
              },
            },
            filePath: "abc",
            id: "abc",
            isHomeFile: true | false,
            jobCount: 1,
            links: {
              abc: "abc", ...
            },
            name: "abc",
            queryable: true | false,
            tags: [
              "abc",
              ...
            ],
            urlPath: "abc",
          },
          ...
        ],
        folders: [
          {
            contents: (ref: NamespaceTree),
            extendedConfig: {
              datasetCount: 1,
              descendants: 1,
              jobCount: 1,
            },
            fileSystemFolder: true | false,
            fullPathList: [
              "abc",
              ...
            ],
            id: "abc",
            isPhysicalDataset: true | false,
            links: {
              abc: "abc", ...
            },
            name: "abc",
            queryable: true | false,
            tags: [
              "abc",
              ...
            ],
            urlPath: "abc",
            version: "abc",
          },
          ...
        ],
        physicalDatasets: [
          {
            datasetConfig: {
              formatSettings: {
                ctime: 1,
                extendedConfig: { /** ByteString **/
                  empty: true | false,
                },
                fullPathList: [
                  "abc",
                  ...
                ],
                location: "abc",
                name: "abc",
                owner: "abc",
                tag: "abc",
                type: "UNKNOWN" | "TEXT" | "JSON" | "CSV" | "TSV" | "PSV" | "AVRO" | "PARQUET" | "HTTP_LOG" | "EXCEL" | "XLS" | "ARROW" | "ICEBERG",
                version: 1,
              },
              fullPathList: [
                "abc",
                ...
              ],
              id: "abc",
              name: "abc",
              tag: "abc",
              type: "INVALID_DATASET_TYPE" | "VIRTUAL_DATASET" | "PHYSICAL_DATASET" | "PHYSICAL_DATASET_SOURCE_FILE" | "PHYSICAL_DATASET_SOURCE_FOLDER" | "PHYSICAL_DATASET_HOME_FILE" | "PHYSICAL_DATASET_HOME_FOLDER",
              version: 1,
            },
            datasetName: "abc",
            jobCount: 1,
            links: {
              abc: "abc", ...
            },
            resourcePath: "abc",
            tags: [
              "abc",
              ...
            ],
          },
          ...
        ],
      },
      ctime: 1,
      datasetCount: 1,
      description: "abc",
      fullPathList: [
        "abc",
        ...
      ],
      id: "abc",
      links: {
        abc: "abc", ...
      },
      name: "abc",
      version: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.model.system.Nodes$NodeInfo`
- Example:
```
{
  cpu: 1.0,
  details: "abc",
  host: "abc",
  ip: "abc",
  isCompatible: true | false,
  isCoordinator: true | false,
  isExecutor: true | false,
  isMaster: true | false,
  memory: 1.0,
  name: "abc",
  nodeTag: "abc",
  port: 1,
  start: any,
  status: "abc",
  version: "abc",
}
```

## `class com.dremio.dac.model.usergroup.UserForm`
- Example:
```
{
  createdAt: 1,
  email: "abc",
  extra: "abc",
  firstName: "abc",
  lastName: "abc",
  modifiedAt: 1,
  password: "abc",
  uid: {
    id: "abc",
  },
  userName: "abc",
  version: "abc",
}
```

## `class com.dremio.dac.model.usergroup.UserLogin`
- Example:
```
{
  password: "abc",
  userName: "abc",
}
```

## `class com.dremio.dac.model.usergroup.UserUI`
- Example:
```
{
  id: "abc",
  links: {
    abc: "abc", ...
  },
  name: "abc",
  resourcePath: "abc",
  userConfig: {
    createdAt: 1,
    email: "abc",
    extra: "abc",
    firstName: "abc",
    lastName: "abc",
    modifiedAt: 1,
    uid: {
      id: "abc",
    },
    userName: "abc",
    version: "abc",
  },
  userName: "abc",
}
```

## `class com.dremio.dac.model.usergroup.UsersUI`
- Example:
```
{
  users: [
    {
      id: "abc",
      links: {
        abc: "abc", ...
      },
      name: "abc",
      resourcePath: "abc",
      userConfig: {
        createdAt: 1,
        email: "abc",
        extra: "abc",
        firstName: "abc",
        lastName: "abc",
        modifiedAt: 1,
        uid: {
          id: "abc",
        },
        userName: "abc",
        version: "abc",
      },
      userName: "abc",
    },
    ...
  ],
}
```

## `class com.dremio.dac.proto.model.acceleration.SystemSettingsApiDescriptor`
- Example:
```
{
  accelerateAggregation: true | false,
  accelerateRaw: true | false,
  layoutRefreshMaxAttempts: 1,
  limit: 1,
  orphanCleanupInterval: 1,
}
```

## `class com.dremio.dac.proto.model.dataset.ExtractListRule`
- Example:
```
any
```

## `class com.dremio.dac.proto.model.dataset.ExtractMapRule`
- Example:
```
{
  path: "abc",
}
```

## `class com.dremio.dac.proto.model.dataset.ExtractRule`
- Example:
```
{
  pattern: {
    ignoreCase: true | false,
    index: 1,
    indexType: "INDEX" | "INDEX_BACKWARDS" | "CAPTURE_GROUP",
    pattern: "abc",
  },
  position: {
    endIndex: { /** Offset **/
      direction: "FROM_THE_START" | "FROM_THE_END",
      value: 1,
    },
    startIndex: { /** Offset **/
      direction: "FROM_THE_START" | "FROM_THE_END",
      value: 1,
    },
  },
  type: "position" | "pattern",
}
```

## `class com.dremio.dac.proto.model.dataset.ReplacePatternRule`
- Example:
```
{
  ignoreCase: true | false,
  selectionPattern: "abc",
  selectionType: "CONTAINS" | "STARTS_WITH" | "ENDS_WITH" | "EXACT" | "MATCHES" | "IS_NULL",
}
```

## `class com.dremio.dac.proto.model.dataset.SplitRule`
- Example:
```
{
  ignoreCase: true | false,
  matchType: "regex" | "exact",
  pattern: "abc",
}
```

## `class com.dremio.dac.resource.ExportProfilesParams`
- Example:
```
{
  chunkSize: 1,
  fromDate: 1,
  outputFilePath: "abc",
  outputFormatType: "ZIP" | "JSON",
  toDate: 1,
  writeMode: "FAIL_IF_EXISTS" | "OVERWRITE" | "SKIP",
}
```

## `class com.dremio.dac.resource.ExportProfilesStats`
- Example:
```
{
  jobsCount: 1,
  outputPath: "abc",
  profilesCount: 1,
  skippedProfilesCount: 1,
}
```

## `class com.dremio.dac.resource.NotificationResponse`
- Example:
```
{
  message: "abc",
  type: "OK" | "INFO" | "WARN" | "ERROR",
}
```

## `class com.dremio.dac.resource.SourcesResource$MetadataImpactingResponse`
- Example:
```
{
  isMetadataImpacting: true | false,
}
```

## `class com.dremio.dac.resource.SystemResource$ResourceInfo`
- Example:
```
{
  averageExecutorCores: 1,
  averageExecutorMemory: 1,
  executorCount: 1,
}
```

## `class com.dremio.dac.service.admin.Setting`
- Example:
```
{
  id: "abc",
  value: any,
}
```

## `class com.dremio.dac.service.admin.SettingsResource$SettingsRequest`
- Example:
```
{
  includeSetSettings: true | false,
  requiredSettings: [
    "abc",
    ...
  ],
}
```

## `class com.dremio.dac.service.admin.SettingsResource$SettingsWrapperObject`
- Example:
```
{
  settings: [
    {
      id: "abc",
      value: any,
    },
    ...
  ],
}
```

## `class com.dremio.dac.support.SupportResponse`
- Example:
```
{
  includesLogs: true | false,
  success: true | false,
  url: "abc",
}
```

## `class com.dremio.dac.util.BackupRestoreUtil$BackupOptions`
- Example:
```
{
  backupDir: "abc",
  binary: true | false,
  includeProfiles: true | false,
}
```

## `class com.dremio.dac.util.BackupRestoreUtil$BackupStats`
- Example:
```
{
  backupPath: "abc",
  files: 1,
  tables: 1,
}
```

## `class com.dremio.file.File`
- Example:
```
{
  fileFormat: {
    fileFormat: {
      ctime: 1,
      fullPath: [
        "abc",
        ...
      ],
      isFolder: true | false,
      location: "abc",
      name: "abc",
      owner: "abc",
      version: "abc",
    },
    id: "abc",
    links: {
      abc: "abc", ...
    },
  },
  filePath: "abc",
  id: "abc",
  isHomeFile: true | false,
  jobCount: 1,
  links: {
    abc: "abc", ...
  },
  name: "abc",
  queryable: true | false,
  tags: [
    "abc",
    ...
  ],
  urlPath: "abc",
}
```

## `class com.dremio.service.namespace.file.FileFormat`
- Example:
```
{
  ctime: 1,
  fullPath: [
    "abc",
    ...
  ],
  isFolder: true | false,
  location: "abc",
  name: "abc",
  owner: "abc",
  version: "abc",
}
```

## `class com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor`
- Example:
```
{
  accelerationGracePeriod: 1,
  accelerationNeverExpire: true | false,
  accelerationNeverRefresh: true | false,
  accelerationRefreshPeriod: 1,
  accelerationTTL: {
    duration: 1,
    unit: "SECONDS" | "MINUTES" | "HOURS" | "DAYS" | "WEEKS" | "MONTHS",
  },
  fieldList: [
    "abc",
    ...
  ],
  method: "FULL" | "INCREMENTAL",
  refreshField: "abc",
}
```

## `class org.glassfish.jersey.server.mvc.Viewable`
- Example:
```
{
  model: any,
  templateName: "abc",
  templateNameAbsolute: true | false,
}
```

## `interface com.dremio.dac.model.job.JobDataFragment`
- Example:
```
{
  columns: [
    {
      index: 1,
      name: "abc",
      type: "TEXT" | "BINARY" | "BOOLEAN" | "FLOAT" | "INTEGER" | "DECIMAL" | "MIXED" | "DATE" | "TIME" | "DATETIME" | "LIST" | "MAP" | "GEO" | "OTHER",
    },
    ...
  ],
  returnedRowCount: 1,
}
```

## `interface com.dremio.provision.ClusterCreateRequest`
- Example:
```
{
  allowAutoStart: true | false,
  allowAutoStop: true | false,
  awsProps: {
    amiId: "abc",
    awsTags: [
      {
        key: "abc",
        value: "abc",
      },
      ...
    ],
    connectionProps: {
      accessKey: "abc",
      assumeRole: "abc",
      authMode: "UNKNOWN" | "AUTO" | "SECRET",
      endpoint: "abc",
      region: "abc",
      secretKey: "abc",
      stsEndpoint: "abc",
    },
    disablePublicIp: true | false,
    extraConfProps: "abc",
    instanceType: "abc",
    nodeIamInstanceProfile: "abc",
    securityGroupId: "abc",
    sshKeyName: "abc",
    subnetId: "abc",
    useClusterPlacementGroup: true | false,
    vpc: "abc",
  },
  clusterType: "YARN" | "MESOS" | "EC2" | "KUBERNETES" | "GCE" | "AZURE",
  dynamicConfig: {
    containerCount: 1,
  },
  name: "abc",
  shutdownInterval: 1,
  yarnProps: {
    distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
    memoryMB: 1,
    queue: "abc",
    secure: true | false,
    subPropertyList: [
      {
        key: "abc",
        type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
        value: "abc",
      },
      ...
    ],
    virtualCoreCount: 1,
  },
}
```

## `interface com.dremio.provision.ClusterModifyRequest`
- Example:
```
{
  allowAutoStart: true | false,
  allowAutoStop: true | false,
  awsProps: {
    amiId: "abc",
    awsTags: [
      {
        key: "abc",
        value: "abc",
      },
      ...
    ],
    connectionProps: {
      accessKey: "abc",
      assumeRole: "abc",
      authMode: "UNKNOWN" | "AUTO" | "SECRET",
      endpoint: "abc",
      region: "abc",
      secretKey: "abc",
      stsEndpoint: "abc",
    },
    disablePublicIp: true | false,
    extraConfProps: "abc",
    instanceType: "abc",
    nodeIamInstanceProfile: "abc",
    securityGroupId: "abc",
    sshKeyName: "abc",
    subnetId: "abc",
    useClusterPlacementGroup: true | false,
    vpc: "abc",
  },
  clusterType: "YARN" | "MESOS" | "EC2" | "KUBERNETES" | "GCE" | "AZURE",
  desiredState: "DELETED" | "RUNNING" | "STOPPED",
  dynamicConfig: {
    containerCount: 1,
  },
  id: "abc",
  name: "abc",
  shutdownInterval: 1,
  tag: "abc",
  yarnProps: {
    distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
    memoryMB: 1,
    queue: "abc",
    secure: true | false,
    subPropertyList: [
      {
        key: "abc",
        type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
        value: "abc",
      },
      ...
    ],
    virtualCoreCount: 1,
  },
}
```

## `interface com.dremio.provision.ClusterResponse`
- Example:
```
{
  allowAutoStart: true | false,
  allowAutoStop: true | false,
  awsProps: {
    amiId: "abc",
    awsTags: [
      {
        key: "abc",
        value: "abc",
      },
      ...
    ],
    connectionProps: {
      accessKey: "abc",
      assumeRole: "abc",
      authMode: "UNKNOWN" | "AUTO" | "SECRET",
      endpoint: "abc",
      region: "abc",
      secretKey: "abc",
      stsEndpoint: "abc",
    },
    disablePublicIp: true | false,
    extraConfProps: "abc",
    instanceType: "abc",
    nodeIamInstanceProfile: "abc",
    securityGroupId: "abc",
    sshKeyName: "abc",
    subnetId: "abc",
    useClusterPlacementGroup: true | false,
    vpc: "abc",
  },
  clusterType: "YARN" | "MESOS" | "EC2" | "KUBERNETES" | "GCE" | "AZURE",
  containers: {
    decommissioningCount: 1,
    decommissioningList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
            value: "abc",
          },
          ...
        ],
      },
      ...
    ],
    disconnectedList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
            value: "abc",
          },
          ...
        ],
      },
      ...
    ],
    pendingCount: 1,
    pendingList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
            value: "abc",
          },
          ...
        ],
      },
      ...
    ],
    provisioningCount: 1,
    runningList: [
      { /** Container **/
        containerId: "abc",
        containerPropertyList: [
          { /** Property **/
            key: "abc",
            type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
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
  dynamicConfig: {
    containerCount: 1,
  },
  error: "abc",
  id: "abc",
  name: "abc",
  shutdownInterval: 1,
  stateChangeTime: 1,
  tag: "abc",
  yarnProps: {
    distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
    memoryMB: 1,
    queue: "abc",
    secure: true | false,
    subPropertyList: [
      { /** Property **/
        key: "abc",
        type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
        value: "abc",
      },
      ...
    ],
    virtualCoreCount: 1,
  },
}
```

## `interface com.dremio.provision.ClusterResponses`
- Example:
```
{
  clusterList: [
    {
      allowAutoStart: true | false,
      allowAutoStop: true | false,
      awsProps: {
        amiId: "abc",
        awsTags: [
          {
            key: "abc",
            value: "abc",
          },
          ...
        ],
        connectionProps: {
          accessKey: "abc",
          assumeRole: "abc",
          authMode: "UNKNOWN" | "AUTO" | "SECRET",
          endpoint: "abc",
          region: "abc",
          secretKey: "abc",
          stsEndpoint: "abc",
        },
        disablePublicIp: true | false,
        extraConfProps: "abc",
        instanceType: "abc",
        nodeIamInstanceProfile: "abc",
        securityGroupId: "abc",
        sshKeyName: "abc",
        subnetId: "abc",
        useClusterPlacementGroup: true | false,
        vpc: "abc",
      },
      clusterType: "YARN" | "MESOS" | "EC2" | "KUBERNETES" | "GCE" | "AZURE",
      containers: {
        decommissioningCount: 1,
        decommissioningList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
                value: "abc",
              },
              ...
            ],
          },
          ...
        ],
        disconnectedList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
                value: "abc",
              },
              ...
            ],
          },
          ...
        ],
        pendingCount: 1,
        pendingList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
                value: "abc",
              },
              ...
            ],
          },
          ...
        ],
        provisioningCount: 1,
        runningList: [
          { /** Container **/
            containerId: "abc",
            containerPropertyList: [
              { /** Property **/
                key: "abc",
                type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
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
      dynamicConfig: {
        containerCount: 1,
      },
      error: "abc",
      id: "abc",
      name: "abc",
      shutdownInterval: 1,
      stateChangeTime: 1,
      tag: "abc",
      yarnProps: {
        distroType: "OTHER" | "APACHE" | "CDH" | "HDP" | "MAPR",
        memoryMB: 1,
        queue: "abc",
        secure: true | false,
        subPropertyList: [
          { /** Property **/
            key: "abc",
            type: "JAVA_PROP" | "SYSTEM_PROP" | "ENV_VAR",
            value: "abc",
          },
          ...
        ],
        virtualCoreCount: 1,
      },
    },
    ...
  ],
  clusterType: "YARN" | "MESOS" | "EC2" | "KUBERNETES" | "GCE" | "AZURE",
}
```

## `interface com.dremio.provision.ResizeClusterRequest`
- Example:
```
{
  containerCount: 1,
}
```


#V2 Others: 
## class com.dremio.dac.explore.bi.PowerBIMessageBodyGenerator
## class com.dremio.dac.explore.bi.QlikAppMessageBodyGenerator
## class com.dremio.dac.explore.bi.TableauMessageBodyGenerator
## class com.dremio.dac.server.DACAuthFilterFeature
## class com.dremio.dac.server.DACExceptionMapperFeature
## class com.dremio.dac.server.DACJacksonJaxbJsonFeature
## class com.dremio.dac.server.FirstTimeFeature
## class com.dremio.dac.server.JSONJobDataFilter
## class com.dremio.dac.server.JSONPrettyPrintFilter
## class com.dremio.dac.server.MediaTypeFilter
## class com.dremio.dac.server.TestResourcesFeature
## class com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper
## class com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper
## class org.glassfish.jersey.media.multipart.MultiPartFeature
## class org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature

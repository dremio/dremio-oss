/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rels;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.StoragePluginId;

public interface ElasticsearchPrel extends Prel {
  BatchSchema getSchema(FunctionLookupContext context);
  StoragePluginId getPluginId();
  ScanBuilder newScanBuilder();
}

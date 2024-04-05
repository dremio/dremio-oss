/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useEffect } from "react";
import { Resource } from "smart-resource1";
import { useResourceSnapshot } from "smart-resource1/react";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { DatasetGraphResource } from "@app/exports/resources/DatasetGraphResource";
import { DatasetGraphResponse } from "@app/exports/types/DatasetGraph.type";
import { useIsArsEnabled } from "@inject/utils/arsUtils";

const logger = getLoggingContext().createLogger("useDatasetGraph");

const DATASET_GRAPHS = "datasetGraphs";

const getDatasetGraphsFromCache = (): Partial<{
  [key: string]: DatasetGraphResponse["datasetGraph"];
}> => {
  const datasetGraphs = sessionStorage.getItem(DATASET_GRAPHS) ?? "{}";

  try {
    return JSON.parse(datasetGraphs);
  } catch (e) {
    logger.error("Error trying to parse dataset graphs from session storage");
    return {};
  }
};

/**
 * @description Fetches and caches dataset graphs
 */
export const useDatasetGraph = (jobId: string, isComplete: boolean) => {
  const [loading, enabled] = useIsArsEnabled();

  const cachedDatasetGraphs = getDatasetGraphsFromCache();
  const cachedGraph = cachedDatasetGraphs[jobId];

  useEffect(() => {
    if (!loading && !enabled && !cachedGraph) {
      DatasetGraphResource.fetch(jobId);
    }
  }, [cachedGraph, enabled, jobId, loading]);

  const datasetGraphSnapshot = useResourceSnapshot(DatasetGraphResource);

  const { value } = datasetGraphSnapshot;

  if (isComplete && !cachedGraph && !!value?.datasetGraph) {
    sessionStorage.setItem(
      DATASET_GRAPHS,
      JSON.stringify({
        ...cachedDatasetGraphs,
        [jobId]: value.datasetGraph,
      }),
    );
  }

  if (!cachedGraph) {
    return datasetGraphSnapshot;
  } else {
    return {
      status: "success",
      value: { datasetGraph: cachedGraph },
      error: null,
    } as Resource<Promise<DatasetGraphResponse>>;
  }
};

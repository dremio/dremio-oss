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
import Immutable from "immutable";

import { Spinner } from "dremio-ui-lib/components";
import Dataset from "./Dataset";
import DatasetGraph from "./DatasetGraph";
import SQL from "../SQL/SQL";

import { store } from "#oss/store/store";
import { ARSFeatureSwitch } from "@inject/utils/arsUtils";
import { useDatasetGraph } from "#oss/exports/providers/useDatasetGraph";
import { DatasetGraphResource } from "#oss/exports/resources/DatasetGraphResource";
import { isSmartFetchLoading } from "#oss/utils/isSmartFetchLoading";
import { addNotification } from "#oss/actions/notification";
// @ts-ignore
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

import "./SQLTab.less";

const { t } = getIntlContext();

type SQLTabProps = {
  algebraicMatch: Immutable.List<any>;
  jobId: string;
  isComplete: boolean;
  submittedSql: string;
};

function SQLTab({
  algebraicMatch,
  jobId,
  isComplete,
  submittedSql,
}: SQLTabProps) {
  const { value, error, status } = useDatasetGraph(jobId, isComplete);
  const datasetGraph = value?.datasetGraph ?? [];

  useEffect(() => {
    if ((error as any)?.responseBody) {
      store.dispatch(
        addNotification((error as any).responseBody.errorMessage, "error", 10),
      );
    }

    return () => {
      DatasetGraphResource.reset();
    };
  }, [error]);

  return (
    <div className="sqlTab">
      <SQL
        sqlString={submittedSql}
        sqlClass="sqlTab__SQLBody"
        title={t("JobDetails.SQL.Submitted")}
      />
      <ARSFeatureSwitch
        renderEnabled={() => null}
        renderDisabled={() => (
          <>
            <span className="sqlTab__SQLGraphHeader">
              {t("JobDetails.SQL.Graph")}
            </span>
            {isSmartFetchLoading(status) ? (
              <Spinner className="flex pt-5 pl-205" />
            ) : (
              <div className="sqlTab__SQLQueryVisualizer">
                {datasetGraph[0]?.description ? (
                  <Dataset description={datasetGraph[0].description} />
                ) : (
                  <DatasetGraph
                    datasetGraph={datasetGraph}
                    algebricMatch={algebraicMatch}
                  />
                )}
              </div>
            )}
          </>
        )}
      />
    </div>
  );
}

export default SQLTab;

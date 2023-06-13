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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import moment from "@app/utils/dayjs";
import { withRouter, type WithRouterProps } from "react-router";
import { useResourceSnapshot } from "smart-resource/react";

import { useArcticCatalogJobs } from "@app/exports/resources/ArcticCatalogJobsResource";
// @ts-ignore
import ArcticCatalogJobsTable from "dremio-ui-common/arctic/components/ArcticCatalogJobsTable/ArcticCatalogJobsTable.js";
import timeUtils from "@app/utils/timeUtils";
import * as PATHS from "@app/exports/paths";
import ArcticCatalogJobsFilters from "./ArcticCatalogJobsFilters/ArcticCatalogJobsFilters";
import {
  ENGINE_SIZE,
  formatArcticJobsCELQuery,
  formatQueryState,
  parseQueryState,
} from "./arctic-catalog-job-utils";
import {
  // JobInfoTypeEnum,
  EngineSize,
  GenericArcticCatalogJob,
} from "@app/exports/endpoints/ArcticCatalogJobs/ArcticCatalogJobs.type";
import { ArcticCatalogsResource } from "@app/exports/resources/ArcticCatalogsResource";
import { intl } from "@app/utils/intl";

import * as classes from "./ArcticCatalogJobs.module.less";

const loadingSkeletonRows = Array(10).fill(null);

type ArcticCatalogJobsProps = WithRouterProps;

export type ArcticCatalogJobsQueryParams = {
  filters: any;
  sort: string;
  order: "ASCENDING" | "DESCENDING";
};

const ArcticCatalogJobs = ({
  location,
  router,
  params,
}: ArcticCatalogJobsProps) => {
  const [arcticCatalogs] = useResourceSnapshot(ArcticCatalogsResource);
  const [pollCount, setPollCount] = useState<number>(0);
  const pollInterval = useRef<any>();
  const curCatalog = useMemo(
    () => arcticCatalogs?.find((c) => c.id === params?.arcticCatalogId),
    [arcticCatalogs, params.arcticCatalogId]
  );
  const query = location?.query;
  const curQuery = useMemo(() => {
    return parseQueryState(query);
  }, [query]);

  const { jobs, isPolling, loadNextPage } = useArcticCatalogJobs(
    params?.arcticCatalogId,
    formatArcticJobsCELQuery(curQuery)
  );

  // Set up refresh for job duration
  useEffect(() => {
    if (isPolling) {
      pollInterval.current = setInterval(() => {
        setPollCount((c: number) => c + 1);
      }, 3000);
    } else {
      clearInterval(pollInterval.current);
    }

    return () => clearInterval(pollInterval.current);
  }, [isPolling]);

  // Memoize jobs
  const [transformedJobs] = useMemo(() => {
    if (jobs) {
      // const targets: any[] = [];
      const tempJobs = (jobs as GenericArcticCatalogJob[]).map(
        (job: GenericArcticCatalogJob) => {
          // const isOptimized = job.type === JobInfoTypeEnum.OPTIMIZE;
          // const tablePath = job.config?.tableId?.split(".");
          // const curTarget = isOptimized
          //   ? `${tablePath[tablePath.length - 1]} (ref: ${
          //       job.config?.reference
          //     })`
          //   : curCatalog?.name ?? params?.arcticCatalogId;

          // if (!targets.find((item) => item?.id === curTarget)) {
          //   targets.push({
          //     label: curTarget,
          //     id: curTarget,
          //     icon: isOptimized
          //       ? "entities/iceberg-table"
          //       : "brand/arctic-catalog-source",
          //     iconStyles: { marginRight: 4 },
          //   });
          // }

          const endTime = job.endedAt
            ? new Date(job.endedAt)?.getTime?.()
            : new Date()?.getTime?.();
          const durationSeconds = job.startedAt
            ? endTime - new Date(job.startedAt)?.getTime?.()
            : null;

          return {
            ...job,
            duration: !durationSeconds
              ? "-"
              : timeUtils.durationWithZero(
                  moment.duration(durationSeconds ?? 0, "ms"),
                  true
                ),
            startedAt: job.startedAt
              ? moment(job.startedAt).format("MMMM D, YYYY h:mmA")
              : "-",
            engineSize: job.engineSize
              ? ENGINE_SIZE[job.engineSize]
              : ENGINE_SIZE[EngineSize.SMALLV1],
            catalogName: curCatalog?.name ?? params?.arcticCatalogId,
          };
        }
      );
      return [tempJobs];
    } else return [loadingSkeletonRows];
  }, [jobs, curCatalog, params.arcticCatalogId, pollCount]);

  useEffect(() => {
    if (Object.keys(query).length === 0) {
      router.push({
        ...location,
        pathname: PATHS.arcticCatalogJobs({
          arcticCatalogId: params?.arcticCatalogId,
        }),
        query: formatQueryState(curQuery),
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update filters by pushing to URL
  const updateQuery = useCallback(
    (query: ArcticCatalogJobsQueryParams) => {
      router.push({
        ...location,
        pathname: PATHS.arcticCatalogJobs({
          arcticCatalogId: params?.arcticCatalogId,
        }),
        query: formatQueryState(query),
      });
    },
    [location, router, params]
  );

  return (
    <div className={classes["arctic-catalog-jobs"]}>
      <ArcticCatalogJobsFilters
        curQuery={curQuery}
        updateQuery={updateQuery}
        // targetFilterItems={targetFilterItems}
      />
      {transformedJobs.length > 0 ? (
        <ArcticCatalogJobsTable
          jobs={transformedJobs}
          order={curQuery.order?.toLowerCase() ?? "none"}
          onScrolledBottom={loadNextPage}
        />
      ) : (
        <div className={classes["arctic-catalog-jobs__none"]}>
          {intl.formatMessage({ id: "ArcticCatalog.Jobs.NoDisplay" })}
        </div>
      )}
    </div>
  );
};

export default withRouter(ArcticCatalogJobs);

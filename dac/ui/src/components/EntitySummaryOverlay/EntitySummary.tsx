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

import clsx from "clsx";
import { useIntl } from "react-intl";
import Immutable from "immutable";

import SummarySubHeader from "../DatasetSummary/components/SummarySubHeader/SummarySubHeader";
import { getIconType } from "../DatasetSummary/datasetSummaryUtils";
import RenderStat from "../DatasetSummary/components/SummaryStats/RenderStat";
import { formatDateTimestampShortNoTZ } from "dremio-ui-common/utilities/formatDate.js";
import { PureEntityIcon } from "#oss/pages/HomePage/components/EntityIcon";
import { ENTITY_TYPES } from "#oss/constants/Constants";
import { Button, Skeleton } from "dremio-ui-lib/components";

import * as summaryClasses from "../DatasetSummary/DatasetSummary.module.less";
import * as summaryHeaderClasses from "../DatasetSummary/components/SummaryItemLabel/SummaryItemLabel.module.less";
import * as summaryStatsClasses from "../DatasetSummary/components/SummaryStats/SummaryStats.module.less";

type EntitySummaryProps = {
  entity: any;
  type: string;
  fullPath: string;
  versionContext: any;
  isLoading: boolean;
  isPanel?: boolean;
  openDetailsPanel?: (dataset: any) => void;
  sourceType?: string;
};

const EntitySummary = ({
  entity,
  type,
  fullPath,
  versionContext,
  isPanel,
  openDetailsPanel,
  sourceType,
  isLoading,
}: EntitySummaryProps) => {
  const { formatMessage } = useIntl();
  return (
    <div className={summaryClasses["dataset-summary-container"]}>
      <div
        className={clsx(
          summaryClasses["dataset-summary-top-section"],
          summaryClasses["dataset-summary-sqleditor"],
        )}
      >
        {!isPanel && (
          <div className={summaryHeaderClasses["dataset-item-header"]}>
            {isLoading ? (
              <Skeleton width="4ch" />
            ) : type === ENTITY_TYPES.source ? (
              <PureEntityIcon
                disableHoverListener
                entityType={type}
                sourceStatus={entity?.state?.status || null}
                sourceType={sourceType}
                style={{ width: 26, height: 26 }}
              />
            ) : (
              <dremio-icon
                class={summaryHeaderClasses["dataset-item-header-icon"]}
                name={`entities/${getIconType(type, !!versionContext)}`}
                style={{ width: 26, height: 26 }}
              />
            )}
            <p className={summaryHeaderClasses["dataset-item-header-title"]}>
              {entity.name}
            </p>
          </div>
        )}
        <SummarySubHeader subTitle={fullPath} versionContext={versionContext} />
        <div className={summaryStatsClasses["summary-stats-container"]}>
          {entity.createdAt && (
            <RenderStat
              title="Common.Created"
              data={
                entity.createdAt &&
                formatDateTimestampShortNoTZ(new Date(entity.createdAt))
              }
            />
          )}
          {entity.ownerEmail && (
            <RenderStat
              title="Common.Owner"
              showAvatar={false}
              wrapContent
              data={entity.ownerEmail}
            />
          )}
        </div>
        {!isPanel && (
          <div
            className={summaryClasses["dataset-summary-button-wrapper"]}
            style={{ justifyContent: "center" }}
          >
            {isLoading ? (
              <Skeleton width="15ch" />
            ) : (
              <Button
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  openDetailsPanel?.(Immutable.fromJS(entity));
                }}
                variant="tertiary"
                disabled={isLoading}
              >
                <dremio-icon
                  name="interface/meta"
                  class={summaryClasses["dataset-summary-open-details-icon"]}
                />
                {formatMessage({ id: "Wiki.OpenDetails" })}
              </Button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default EntitySummary;

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
import { useState } from "react";
import { useIntl } from "react-intl";
import { Skeleton } from "dremio-ui-lib/components";
import { Avatar, Tooltip } from "dremio-ui-lib/components";
import * as classes from "./RenderStat.module.less";
import { nameToInitials } from "#oss/exports/utilities/nameToInitials";

const RenderData = ({
  data,
  showAvatar,
}: {
  data: any;
  showAvatar?: boolean;
}) => {
  if (showAvatar) {
    const initials = nameToInitials(data);
    return (
      <>
        <Avatar initials={initials} />
        <span className={classes["data"]}>{data}</span>
      </>
    );
  } else {
    return data;
  }
};

const RenderStat = ({
  title,
  data,
  showAvatar,
  wrapContent,
}: {
  title: string;
  data: any;
  style?: any;
  showAvatar?: boolean;
  wrapContent?: boolean;
}) => {
  const { formatMessage } = useIntl();
  const [showTooltip, setShowTooltip] = useState(false);

  return data === undefined ? null : (
    <dl>
      <div className={classes["summary-stats"]}>
        <dt>{formatMessage({ id: title })}</dt>
        {showTooltip ? (
          <Tooltip content={data} interactive>
            <dd className={wrapContent ? classes["data-container"] : ""}>
              {data || data === 0 ? (
                <RenderData data={data} showAvatar={showAvatar} />
              ) : (
                <Skeleton width="12ch" className="ml-1" />
              )}
            </dd>
          </Tooltip>
        ) : (
          <dd
            className={wrapContent ? classes["data-container"] : ""}
            ref={(elem: any) => {
              if (elem?.offsetWidth < elem?.scrollWidth) {
                setShowTooltip(true);
              }
            }}
          >
            {data || data === 0 ? (
              <RenderData data={data} showAvatar={showAvatar} />
            ) : (
              <Skeleton width="12ch" className="ml-1" />
            )}
          </dd>
        )}
      </div>
    </dl>
  );
};

export default RenderStat;

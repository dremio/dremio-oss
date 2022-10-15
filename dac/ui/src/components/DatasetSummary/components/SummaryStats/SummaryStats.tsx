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
import * as classes from "./SummaryStats.module.less";
import { intl } from "@app/utils/intl";
import LinkWithHref from "@app/components/LinkWithRef/LinkWithRef";
import LoadingBar from "@app/components/LoadingBar/LoadingBar";

type SummaryStatsProps = {
  jobsLink: string;
  jobCount: number;
  descendantsCount: number;
  location: Record<string, any>;
};

const SummaryStats = ({
  jobsLink,
  jobCount,
  descendantsCount,
  location,
}: SummaryStatsProps) => {
  const { formatMessage } = intl;
  const currentRoute = location.pathname + location?.search;
  // reloads the page if the jobs link is the same as the current location
  const onJobClick = () => {
    if (currentRoute === jobsLink) {
      window.location.reload();
    }
  };

  return (
    <div className={classes["summary-stats-container"]}>
      <div className={classes["summary-stats"]}>
        <span>{formatMessage({ id: "Job.Jobs" })}</span>
        {jobCount || jobCount === 0 ? (
          // @ts-ignore
          <LinkWithHref onClick={onJobClick} to={jobsLink}>
            {jobCount}
          </LinkWithHref>
        ) : (
          <LoadingBar width={144} height={12} />
        )}
      </div>

      <div className={classes["summary-stats"]}>
        <span>{formatMessage({ id: "Dataset.Descendants" })}</span>
        {descendantsCount || descendantsCount === 0 ? (
          descendantsCount
        ) : (
          <LoadingBar width={144} height={12} />
        )}
      </div>
    </div>
  );
};

export default SummaryStats;

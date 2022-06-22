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

import { cloneDeep } from "lodash";
import { FormattedMessage } from "react-intl";
import { QueryStatusType } from "../components/SqlEditor/SqlQueryTabs/utils";

type StatusObjectType = {
  renderIcon?: string;
  text?: any;
  buttonFunc?: () => void;
  buttonText?: string;
  buttonIcon?: string;
  buttonAlt?: string;
  ranJob?: boolean;
  error?: any;
};

export const assemblePendingOrRunningTabContent = (
  queryStatuses: QueryStatusType[],
  statusesArray: string[],
  newQueryStatuses: (statuses: { statuses: QueryStatusType[] }) => {},
  cancelJob: (jobId: string | undefined) => {},
  cancelPendingSql: (index: number) => void
) => {
  const tabStatusArr = queryStatuses.map((query, index) => {
    const obj = {} as StatusObjectType;

    if (statusesArray[index] === "RUNNING") {
      obj.renderIcon = statusesArray[index];
      obj.text = <FormattedMessage id="NewQuery.Running" />;
      obj.buttonFunc = () => {
        const newStutuses = cloneDeep(queryStatuses);
        newStutuses[index].isCancelDisabled = true;
        newQueryStatuses({ statuses: newStutuses });
        cancelJob(query.jobId);
      };
      obj.buttonText = "Cancel Job";
      obj.buttonIcon = "CancelJobAndStop.svg";
      obj.buttonAlt = "Cancel a job from running button";
      obj.ranJob = true;
    } else if (!query.cancelled && !query.jobId && !query.error) {
      obj.renderIcon = "RUNNING";
      obj.text = <FormattedMessage id="NewQuery.Waiting" />;
      obj.buttonFunc = () => cancelPendingSql(index);
      obj.buttonText = "Remove Query";
      obj.buttonIcon = "TrashBold.svg";
      obj.buttonAlt = "Remove a query before running button";
    } else if (query.cancelled && !query.error) {
      obj.text = <FormattedMessage id="NewQuery.Removed" />;
    } else if (query.error) {
      obj.error = query.error;
    } else {
      return;
    }

    return obj;
  });

  return tabStatusArr;
};

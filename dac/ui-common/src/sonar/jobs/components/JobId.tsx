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
/* eslint-disable react/prop-types */
import { FC } from "react";
import { Job } from "@dremio/dremio-js/interfaces";
import * as jobPaths from "../../../paths/jobs";
import { getSonarContext } from "../../../contexts/SonarContext";
import { CopyButton, Tooltip } from "dremio-ui-lib/components";
import clsx from "clsx";

type JobIdProps = {
  className?: string;
  job: Job;
  truncate?: boolean;
};

export const createJobId = (Link: FC<Record<string, any>>): FC<JobIdProps> =>
  function JobId(props) {
    return (
      <div className={clsx(props.className, "show-on-hover__target")}>
        <Link
          className="font-mono flex items-center h-full"
          to={jobPaths.job.link({
            // @ts-ignore
            projectId: getSonarContext().getSelectedProjectId?.(),
            jobId: props.job.id,
          })}
        >
          {!props.truncate && props.job.id}

          {props.truncate && (
            <Tooltip
              content={<span className="font-mono">{props.job.id}</span>}
              portal
            >
              <span style={{ userSelect: "none" }}>
                …{props.job.id.slice(-12)}
              </span>
            </Tooltip>
          )}
        </Link>
        <span className="show-on-hover">
          <CopyButton contents={props.job.id} />
        </span>
      </div>
    );
  };

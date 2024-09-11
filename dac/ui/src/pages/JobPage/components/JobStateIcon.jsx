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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import { Tooltip } from "dremio-ui-lib";

@injectIntl
export default class JobStateIcon extends PureComponent {
  static propTypes = {
    state: PropTypes.string.isRequired,
    intl: PropTypes.object.isRequired,
    style: PropTypes.object,
  };

  render() {
    const state = this.props.state;

    let src = "ellipsis";
    let className = "";

    if (JobStatusIcons[state]) {
      if (typeof JobStatusIcons[state] === "string") {
        src = JobStatusIcons[state];
      } else {
        src = JobStatusIcons[state].src;
        className = JobStatusIcons[state].className;
      }
    }

    const dimension = src === "planning" ? 22 : 24;

    return (
      <Tooltip
        title={this.props.intl.formatMessage({ id: "Job.State." + state })}
      >
        <dremio-icon
          name={`job-state/${src}`}
          alt={this.props.intl.formatMessage({ id: "Job.State." + state })}
          class={className}
          style={{
            height: dimension,
            width: dimension,
            verticalAlign: "unset",
          }}
        />
      </Tooltip>
    );
  }
}

export const JobStatusIcons = {
  NOT_SUBMITTED: "ellipsis",
  STARTING: "starting",
  RUNNING: { src: "running", className: "spinner" },
  COMPLETED: "job-completed",
  CANCELED: "canceled",
  FAILED: "error-solid",
  CANCELLATION_REQUESTED: "cancelled-gray",
  ENQUEUED: "ellipsis",
  PLANNING: "planning",
  PENDING: "setup",
  METADATA_RETRIEVAL: "planning",
  QUEUED: "queued",
  ENGINE_START: "engine-start",
  EXECUTION_PLANNING: "starting",
};

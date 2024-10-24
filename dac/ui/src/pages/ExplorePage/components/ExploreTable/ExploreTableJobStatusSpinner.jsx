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
import { Component } from "react";
import PropTypes from "prop-types";

import Spinner from "#oss/components/Spinner";
import socket from "@inject/utils/socket";

import { isWorking, JOB_STATUS } from "./ExploreTableJobStatus";

export default class ExploreTableJobStatusSpinner extends Component {
  static propTypes = {
    jobProgress: PropTypes.object,
    jobId: PropTypes.string,
    action: PropTypes.string,
    message: PropTypes.string,
  };

  prevState = {
    status: null,
    recordCount: 0,
  };

  componentDidUpdate(prevProps) {
    const { jobProgress, jobId } = this.props;
    if (!jobProgress) return;

    const { jobProgress: prevJobProgress } = prevProps;
    const { status } = jobProgress;
    if (
      status === JOB_STATUS.running &&
      (!prevJobProgress || prevJobProgress.status !== JOB_STATUS.running)
    ) {
      // if switched to running start listen to row count
      jobId && socket.startListenToJobRecords(jobId);
    } else if (
      status !== JOB_STATUS.running &&
      prevJobProgress &&
      prevJobProgress.status === JOB_STATUS.running
    ) {
      // if switched from running stop listen to row count
      prevProps.jobId && socket.stopListenToJobRecords(prevProps.jobId);
    }
  }

  renderProgressIcon = (jobProgress, action) => {
    if (!jobProgress || !isWorking(jobProgress.status)) {
      return null;
    }

    if (action === "run") {
      return (
        <Spinner
          iconStyle={styles.iconSpinner}
          style={styles.spinnerBase}
          message={this.props.message}
        />
      );
    }

    return null;
  };

  render() {
    const { jobProgress, action } = this.props;
    return this.renderProgressIcon(jobProgress, action);
  }
}

const styles = {
  spinnerBase: {
    position: "relative",
    width: 28,
    height: 28,
  },
  iconSpinner: {
    marginRight: -3,
  },
};

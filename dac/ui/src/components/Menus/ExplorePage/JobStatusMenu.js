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
import { Link } from "react-router";

import jobsUtils from "@app/utils/jobsUtils.js";

import Menu from "./Menu";
import MenuItem from "./MenuItem";

export class JobStatusMenu extends PureComponent {
  static propTypes = {
    action: PropTypes.func,
    isCancellable: PropTypes.bool,
    closeMenu: PropTypes.func,
    jobId: PropTypes.string,
  };

  clickCancel = () => {
    this.props.closeMenu();
    this.props.action("cancel");
  };

  render() {
    const { isCancellable, jobId } = this.props;
    const isNewJobsPage = jobsUtils.isNewJobsPage();
    return (
      <Menu>
        <MenuItem key="job-details">
          <Link
            to={{
              pathname: isNewJobsPage ? `/job/${jobId}` : "/jobs",
              hash: isNewJobsPage ? null : `#${jobId}`,
              state: {
                selectedJobId: jobId,
                isFromJobListing: false,
              },
            }}
          >
            {la("View Details")}
          </Link>
        </MenuItem>
        <MenuItem
          key="job-cancel"
          onClick={this.clickCancel}
          disabled={!isCancellable}
        >
          {la("Cancel Job")}
        </MenuItem>
      </Menu>
    );
  }
}

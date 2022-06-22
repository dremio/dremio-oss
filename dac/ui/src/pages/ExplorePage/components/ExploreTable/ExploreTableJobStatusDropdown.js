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
import DropdownMenu from "@app/components/Menus/DropdownMenu";
import { JobStatusMenu } from "@app/components/Menus/ExplorePage/JobStatusMenu";
import PropTypes from "prop-types";

const ExploreTableJobStatusDropdown = ({
  jobId,
  jobStatusName = "",
  isJobCancellable,
  cancelJob,
}) => {
  const doButtonAction = (actionType) => {
    if (!jobId) return;

    if (actionType === "cancel") {
      cancelJob(jobId);
    } //else ignore
  };

  return (
    <span className="exploreJobStatus__value">
      {jobId && (
        <DropdownMenu
          className="exploreJobStatus__button"
          hideArrow
          hideDivider
          style={styles.textLink}
          textStyle={styles.menuText}
          text={jobStatusName}
          menu={
            <JobStatusMenu
              action={doButtonAction}
              jobId={jobId}
              isCancellable={isJobCancellable}
            />
          }
        />
      )}
    </span>
  );
};

ExploreTableJobStatusDropdown.propTypes = {
  jobId: PropTypes.string.isRequired,
  jobStatusName: PropTypes.string,
  isJobCancellable: PropTypes.bool.isRequired,
  cancelJob: PropTypes.func.isRequired,
};

export default ExploreTableJobStatusDropdown;

const styles = {
  textLink: {
    color: "#43B8C9",
    marginRight: 0,
  },
  menuText: {
    marginRight: 0,
  },
};

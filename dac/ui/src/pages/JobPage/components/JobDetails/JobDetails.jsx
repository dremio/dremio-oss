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
import Immutable from "immutable";

import ViewStateWrapper from "components/ViewStateWrapper";

import { flexElementAuto } from "#oss/uiTheme/less/layout.less";

import HeaderDetails from "./HeaderDetails";
import TabsNavigation from "./TabsNavigation";
import TabsContent from "./TabsContent";
import "./JobDetails.less";

class JobDetails extends PureComponent {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    jobId: PropTypes.string,
    location: PropTypes.object,
    askGnarly: PropTypes.func,

    // actions
    cancelJob: PropTypes.func,
    downloadFile: PropTypes.func,
    showJobProfile: PropTypes.func,
    downloadJobProfile: PropTypes.func,

    //connected
    viewState: PropTypes.instanceOf(Immutable.Map),
  };

  defaultProps = {
    jobDetails: Immutable.Map(),
  };

  state = {
    activeTab: "overview",
  };

  changeTab = (id) => this.setState({ activeTab: id });

  render() {
    const { jobId, jobDetails, viewState } = this.props;
    const haveDetails = !!(jobId && jobDetails && !jobDetails.isEmpty());

    return (
      <section className="job-details" style={styles.base}>
        <ViewStateWrapper
          viewState={viewState}
          hideChildrenWhenInProgress={false}
          hideChildrenWhenFailed={false}
          hideSpinner={haveDetails}
        >
          {haveDetails && (
            <div
              style={{
                height: "100%",
                display: "flex",
                flexDirection: "column",
              }}
            >
              <HeaderDetails
                cancelJob={this.props.cancelJob}
                jobId={jobId}
                style={styles.header}
                jobDetails={jobDetails}
                downloadFile={this.props.downloadFile}
              />
              <TabsNavigation
                changeTab={this.changeTab}
                activeTab={this.state.activeTab}
                style={styles.header}
                attemptDetails={jobDetails.get("attemptDetails")}
                showJobProfile={this.props.showJobProfile}
                location={this.props.location}
              />
              <TabsContent
                jobId={jobId}
                jobDetails={jobDetails}
                showJobProfile={this.props.showJobProfile}
                changeTab={this.changeTab}
                activeTab={this.state.activeTab}
                className={flexElementAuto} // take a rest of available space
                downloadJobProfile={this.props.downloadJobProfile}
              />
            </div>
          )}
        </ViewStateWrapper>
      </section>
    );
  }
}

const styles = {
  base: {
    position: "relative",
    display: "flex",
    flexDirection: "column",
    margin: "0 0 0 10px",
    overflowY: "auto",
    flex: "1 1",
    background: "var(--fill--primary)",
  },
  header: {
    flexShrink: 0,
  },
};
export default JobDetails;

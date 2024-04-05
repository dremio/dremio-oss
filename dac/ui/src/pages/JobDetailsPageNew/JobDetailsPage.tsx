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
import { useState, useEffect, useRef } from "react";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router";
import { compose } from "redux";
import { injectIntl, useIntl } from "react-intl";
import DocumentTitle from "react-document-title";
import Immutable from "immutable";
import { v4 as uuidv4 } from "uuid";
import {
  loadJobDetails,
  JOB_DETAILS_VIEW_ID,
  fetchJobExecutionOperatorDetails,
  clearJobProfileData,
} from "actions/joblist/jobList";
import {
  showJobProfile,
  cancelJobAndShowNotification,
  showReflectionJobProfile,
} from "actions/jobs/jobs";
import { updateViewState } from "actions/resources";
import { downloadFile } from "sagas/downloadFile";
import { getViewState } from "selectors/resources";
import ViewStateWrapper from "components/ViewStateWrapper";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import jobsUtils from "@app/utils/jobsUtils";
import { renderContent } from "dyn-load/utils/jobsUtils";
import socket from "@inject/utils/socket";
import { GetIsSocketForSingleJob } from "@inject/pages/JobDetailsPageNew/utils";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";

import TopPanel from "./components/TopPanel/TopPanel";
import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";

import { jobDetailsTabs, getIconName } from "dyn-load/utils/jobsUtils";
import { TabPanel, Tab, TabList, useTabList } from "dremio-ui-lib/components";
// @ts-ignore
import { getPrivilegeContext } from "dremio-ui-common/contexts/PrivilegeContext.js";
import { store } from "@app/store/store";

import "./JobDetailsPage.less";

const POLL_INTERVAL = 3000;

type ConnectedProps = {
  jobId: string;
  totalAttempts: number;
  viewState: any;
  jobDetailsFromStore: any;

  getJobDetails: typeof loadJobDetails;
  getViewStateDetails: typeof updateViewState;
  showJobIdProfile: typeof showJobProfile;
  showReflectionJobIdProfile: typeof showReflectionJobProfile;
  downloadJobFile: typeof downloadFile;
  cancelJob: typeof cancelJobAndShowNotification;
  getJobExecutionOperatorDetails: typeof fetchJobExecutionOperatorDetails;
  resetJobProfileData: typeof clearJobProfileData;
};

const JobDetailsPage = (
  props: ConnectedProps & RouteComponentProps<any, any>,
) => {
  const { formatMessage } = useIntl();
  const isSqlContrast = localStorageUtils?.getSqlThemeContrast();
  const [isContrast, setIsContrast] = useState(isSqlContrast);
  const [jobDetails, setJobDetails] = useState(Immutable.Map());
  const [pollId, setPollId] = useState<NodeJS.Timer | null>(null);
  const [isListeningForProgress, setIsListeningForProgress] = useState(false);

  const {
    getTabProps,
    getTabPanelProps,
    tab: currentTab,
  } = useTabList("Overview");

  const {
    router,
    location,
    jobId,
    downloadJobFile,
    viewState,
    getJobDetails,
    showJobIdProfile,
    showReflectionJobIdProfile,
    cancelJob,
    getViewStateDetails,
    jobDetailsFromStore,
    getJobExecutionOperatorDetails,
    resetJobProfileData,
  } = props;

  const propsForRenderContent = {
    jobDetails,
    downloadJobFile,
    isContrast,
    setIsContrast,
    jobDetailsFromStore,
    showJobIdProfile,
    showReflectionJobIdProfile,
    jobId,
    getJobExecutionOperatorDetails,
    location,
  };

  useEffect(() => {
    if (GetIsSocketForSingleJob()) socket.setServerHeartbeat(true);
    return () => {
      if (GetIsSocketForSingleJob()) socket.setServerHeartbeat(false);
    };
  }, []);

  //Effect should use latest query string when polling runs
  const loc = useRef<any>();
  loc.current = location;
  const reflectionId = location?.hash?.replace("#", "");
  const isAdmin = (
    getPrivilegeContext() as { isAdmin: () => boolean }
  ).isAdmin();
  const jobDetailsTabsForPage = reflectionId
    ? jobDetailsTabs.filter(
        (tab) =>
          tab === "Overview" ||
          tab === "Profile" ||
          (tab === "Execution" && isAdmin),
      )
    : jobDetailsTabs;

  // TODO: Revisit this to fetch the info from socket instead of making multiple calls to get job details
  useEffect(() => {
    if (GetIsSocketForSingleJob()) {
      const { query: { attempts = 1 } = {} } = location || {};

      const skipStartAction =
        jobDetails && jobDetails.size !== 0 && jobDetails.get("id") === jobId;
      fetchJobDetails(skipStartAction);
      const jobAttempt = jobDetailsFromStore
        ? jobDetailsFromStore.get("totalAttempts")
        : attempts;
      router.replace({
        ...location,
        query: {
          ...loc.current.query,
          attempts: jobAttempt,
        },
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId, jobDetailsFromStore]);

  useEffect(() => {
    const pollJobDetails = async () => {
      const firstResponse = await fetchJobDetails();
      if (
        firstResponse &&
        jobsUtils.isJobRunning(firstResponse && firstResponse.jobStatus)
      ) {
        const id = setInterval(async () => {
          const { query: { attempts = 1 } = {} } = loc.current || {};
          let jobAttempts = attempts;
          const response = await fetchJobDetails(true);
          if (
            response &&
            response.totalAttempts &&
            attempts !== response.totalAttempts
          ) {
            jobAttempts = response.totalAttempts;
          } else if (
            response &&
            response.attemptDetails &&
            attempts !== response.attemptDetails.length
          ) {
            jobAttempts = response.attemptDetails.length;
          }
          router.replace({
            ...loc.current,
            query: {
              ...loc.current.query,
              attempts: jobAttempts,
            },
          });
          const status = response?.jobStatus || response?.state || true;
          if (!jobsUtils.isJobRunning(status)) {
            clearInterval(id);
          }
        }, POLL_INTERVAL);
        setPollId(id);
      }
    };
    if (!GetIsSocketForSingleJob() && jobId) {
      pollJobDetails();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  useEffect(() => {
    if (isListeningForProgress) {
      return () => socket.stoptListenToQVJobProgress(jobId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isListeningForProgress]);

  useEffect(() => {
    if (pollId) {
      return () => clearInterval(pollId);
    }
  }, [pollId]);

  useEffect(() => {
    return () => (resetJobProfileData as () => void)();
  }, [resetJobProfileData]);

  const fetchJobDetails = async (skipStartAction?: boolean) => {
    const { query: { attempts = 1 } = {} } = location || {};
    // Skip start action for updates to the same job to avoid screen flickering (due to the spinner)
    const response = (await getJobDetails(
      jobId,
      JOB_DETAILS_VIEW_ID,
      attempts,
      skipStartAction,
    )) as any;
    if (!response) return; // no-payload error

    if (!response.error) {
      if (
        GetIsSocketForSingleJob() &&
        (jobDetails.size === 0 || jobDetails.get("id") !== jobId) &&
        jobsUtils.isJobRunning(response.jobStatus)
      ) {
        socket.startListenToQVJobProgress(jobId);
        setIsListeningForProgress(true);
      }

      // SQL tabs changed the jobList to only update if a job already exists.
      // This forces the job details to be stored in the jobList when the list is empty
      if (
        GetIsSocketForSingleJob() &&
        !jobDetailsFromStore &&
        jobsUtils.isJobRunning(response.jobStatus)
      ) {
        store.dispatch({ type: "SET_JOB_LIST", jobList: [response] });
      }

      setJobDetails(Immutable.fromJS(response));
    } else if (response.status === 404) {
      const errorMessage = formatMessage({ id: "Job.Details.NoData" });
      getViewStateDetails(JOB_DETAILS_VIEW_ID, {
        isFailed: false,
        isWarning: true,
        isInProgress: false,
        error: {
          message: errorMessage,
          id: uuidv4(),
        },
      });
    }
    return response;
  };

  const jobStatus =
    jobDetailsFromStore && GetIsSocketForSingleJob()
      ? jobDetailsFromStore.get("state")
      : jobDetails.get("jobStatus") || jobDetails.get("state");

  const attemptDetails = jobDetails?.get("attemptDetails") as any;
  const profileUrl = attemptDetails?.getIn([0, "profileUrl"]);
  const isSingleProfile = attemptDetails?.size === 1;
  const isVisualProfile = currentTab === "Execution";

  return (
    <div style={{ height: "100%" }}>
      <div className={"jobsPageBody"}>
        <DocumentTitle title={formatMessage({ id: "Job.JobDetails" })} />
        <SonarSideNav />
        <div className={"jobPageContentDiv"}>
          <NavCrumbs />
          <ViewStateWrapper
            hideChildrenWhenFailed={false}
            viewState={viewState}
          >
            {jobDetails.get("id") && (
              <div className="jobDetails">
                <div className="jobDetails__topPanel">
                  <TopPanel
                    jobId={jobDetails.get("id") as string}
                    jobStatus={jobStatus}
                    jobDetails={jobDetails}
                    cancelJob={cancelJob}
                    renderTabs={() => {
                      return (
                        <TabList
                          aria-label={formatMessage({
                            id: "Job.Details.Tabs.AriaLabel",
                          })}
                        >
                          {jobDetailsTabsForPage.map((id, index) => {
                            const { onClick, ...rest } = getTabProps({
                              id,
                              controls: `${id}Panel`,
                            });
                            return (
                              <Tab
                                key={`${id}-${index}`}
                                {...rest}
                                onClick={() => {
                                  if (id === "Profile" && isSingleProfile) {
                                    reflectionId
                                      ? showReflectionJobIdProfile(
                                          profileUrl,
                                          reflectionId,
                                        )
                                      : showJobIdProfile(profileUrl);
                                  } else {
                                    onClick();
                                  }
                                }}
                              >
                                <div className="dremio-icon-label">
                                  <dremio-icon
                                    alt={id}
                                    name={getIconName(id)}
                                    data-qa={getIconName(id)}
                                  />
                                  {formatMessage({ id: `TopPanel.${id}` })}
                                </div>
                              </Tab>
                            );
                          })}
                        </TabList>
                      );
                    }}
                  />
                </div>
                {jobDetailsTabsForPage.map((curTab) => {
                  return (
                    <TabPanel
                      key={`${curTab}Panel`}
                      className={`${
                        isVisualProfile
                          ? ""
                          : "gutter-left--double gutter-right--double"
                      } full-height jobDetails__bottomPanel`}
                      {...getTabPanelProps({
                        id: `${curTab}Panel`,
                        labelledBy: curTab,
                      })}
                    >
                      {renderContent(currentTab, propsForRenderContent)}
                    </TabPanel>
                  );
                })}
              </div>
            )}
          </ViewStateWrapper>
        </div>
      </div>
    </div>
  );
};

function mapStateToProps(
  state: Record<string, any>,
  ownProps: RouteComponentProps<any, any>,
) {
  const { routeParams: { jobId } = {} } = ownProps;

  const jobsList = state.jobs.jobs.get("jobList").toArray();
  const currentJob = jobsList.find((job: any) => {
    return job.get("id") === jobId;
  });
  const totalAttempts = currentJob ? currentJob.get("totalAttempts") : 1;
  return {
    jobId,
    totalAttempts,
    jobDetailsFromStore: currentJob,
    viewState: getViewState(state, JOB_DETAILS_VIEW_ID),
  };
}

const mapDispatchToProps = {
  getJobDetails: loadJobDetails,
  showJobIdProfile: showJobProfile,
  showReflectionJobIdProfile: showReflectionJobProfile,
  getViewStateDetails: updateViewState,
  cancelJob: cancelJobAndShowNotification,
  downloadJobFile: downloadFile,
  getJobExecutionOperatorDetails: fetchJobExecutionOperatorDetails,
  resetJobProfileData: clearJobProfileData,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps),
  injectIntl,
)(JobDetailsPage);

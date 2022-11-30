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
import clsx from "clsx";
import { useState, useEffect } from "react";
import { connect } from "react-redux";
import { compose } from "redux";
import { withRouter } from "react-router";
import Immutable from "immutable";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import { flexElementAuto } from "@app/uiTheme/less/layout.less";
import { getClusterInfo } from "@app/utils/infoUtils";
import { getSupport } from "@app/utils/supportUtils";
import { usePrevious } from "@app/utils/jobsUtils";
import { getExploreState } from "@app/selectors/explore";

import { updateQueryState, setClusterType } from "actions/jobs/jobs";
import {
  fetchJobsList,
  loadItemsForFilter,
  loadNextJobs,
  JOB_PAGE_NEW_VIEW_ID,
} from "actions/joblist/jobList";
import { getJobList, getDataWithItemsForFilters } from "selectors/jobs";
import { getViewState } from "selectors/resources";
import { isEqual, isEmpty } from "lodash";

import { parseQueryState } from "utils/jobsQueryState";

import JobsContent from "./components/JobsContent";
import * as PATHS from "@app/exports/paths";

import "./JobPageNew.less";

const JobListingPage = (props) => {
  const {
    queryState,
    viewState,
    router,
    location,
    next,
    isNextJobsInProgress,
    dataFromUserFilter,
    dataWithItemsForFilters,
    admin,
    jobList,
    loadNextJobsList,
    dispatchFetchJobsList,
    dispatchUpdateQueryState,
    dispatchLoadItemsForFilter,
    dispatchSetClusterType,
    jobsColumns,
    isFromExplorePage,
    renderButtons,
    handleTabChange,
    jobIdList,
    queryFilter,
    children,
  } = props;
  const { state: { isFromJobListing } = {} } = location || {};
  const [lastLoaded, setLastLoaded] = useState("");
  const [previousJobId, setPreviousJobId] = useState("");
  const prevQueryState = usePrevious(queryState);

  useEffect(() => {
    handleCluster();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!isEmpty(location.query) && !isEqual(queryState, prevQueryState)) {
      setLastLoaded("");
      if (!isFromExplorePage)
        dispatchFetchJobsList(queryState, JOB_PAGE_NEW_VIEW_ID);
    }
  }, [queryState]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (
      isEmpty(location.query?.filters) &&
      (location.pathname === "/jobs" ||
        location.pathname.startsWith("/jobs/job"))
    ) {
      if (!queryState.equals(prevQueryState)) {
        dispatchUpdateQueryState(
          queryState.setIn(["filters", "qt"], ["UI", "EXTERNAL"])
        );
      }
    }
  }, [location]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleCluster = async () => {
    const clusterInfo = await getClusterInfo();
    const supportInfo =
      getSupport(admin) !== undefined ? getSupport(admin) : false;
    const data = {
      clusterType: clusterInfo.clusterType,
      isSupport: supportInfo,
    };
    clusterInfo.clusterType !== undefined
      ? dispatchSetClusterType(data)
      : dispatchSetClusterType("NP");
  };

  const changePages = (data) => {
    const { state: locationState } = location;
    const currentJobId = data && data.rowData.data.job.value;
    const selectedJob = jobList.find((job) => job.get("id") === currentJobId);
    const attempts = selectedJob ? selectedJob.get("totalAttempts") : 1;
    const currentJobURL = PATHS.job({ jobId: currentJobId });
    if (data !== null && location.pathname !== currentJobURL) {
      router.push({
        ...location,
        pathname: currentJobURL,
        search: null,
        query: {
          ...location.query,
          attempts,
        },
        hash: null,
        state: {
          ...locationState,
          selectedJobId: currentJobId,
          isFromJobListing: true,
          history: {
            ...location,
          },
        },
      });
      return;
    }
    if (isFromJobListing) {
      router.goBack();
    } else {
      router.push({
        ...location,
        state: {
          ...locationState,
          selectedJobId: null,
        },
      });
    }
  };

  let recentJobId = "";
  const tableRowRenderer = (index) => {
    const lastJob = jobList.get(index);
    const lastJobId = lastJob && lastJob.get("id");
    const parsedNextAPI = next && next.split("/");
    const oldAPIOnNewJobsPage =
      (parsedNextAPI && parsedNextAPI[1] === "jobs") || isFromExplorePage;

    if (
      index + 1 === jobList.size &&
      next &&
      lastLoaded !== next &&
      recentJobId !== lastJobId &&
      !oldAPIOnNewJobsPage
    ) {
      loadNextJobsList(next, JOB_PAGE_NEW_VIEW_ID);
      setLastLoaded(next);
      lastJob && setPreviousJobId(lastJobId);
      recentJobId = lastJobId;
    }
  };

  return (
    <div className={"jobPageNew"}>
      <span className={clsx("content-wrapper", { hasChildren: !!children })}>
        <JobsContent
          className={flexElementAuto} // Page object adds flex in style
          loadNextJobs={tableRowRenderer}
          // todo: update to react-router v3 so don't have to deep pass `location` anymore
          location={location}
          jobId={previousJobId}
          jobs={jobList}
          queryState={queryState}
          next={next}
          isNextJobsInProgress={isNextJobsInProgress}
          viewState={viewState}
          onUpdateQueryState={dispatchUpdateQueryState}
          loadItemsForFilter={dispatchLoadItemsForFilter}
          dataFromUserFilter={dataFromUserFilter}
          dataWithItemsForFilters={dataWithItemsForFilters}
          changePages={changePages}
          showSideNav={isFromExplorePage ? false : !children}
          jobsColumns={jobsColumns}
          isFromExplorePage={isFromExplorePage}
          renderButtons={renderButtons}
          handleTabChange={handleTabChange}
          router={router}
          exploreJobIdList={jobIdList}
          queryFilter={queryFilter}
        />
      </span>
      {children}
    </div>
  );
};

JobListingPage.propTypes = {
  router: PropTypes.object.isRequired,
  location: PropTypes.object.isRequired,
  jobId: PropTypes.string,
  jobList: PropTypes.instanceOf(Immutable.List),
  explorePageJobsPending: PropTypes.instanceOf(Immutable.List),
  queryState: PropTypes.instanceOf(Immutable.Map).isRequired,
  next: PropTypes.string,
  viewState: PropTypes.instanceOf(Immutable.Map),
  isNextJobsInProgress: PropTypes.bool,
  dataFromUserFilter: PropTypes.array,
  dataWithItemsForFilters: PropTypes.object,
  clusterType: PropTypes.string,
  admin: PropTypes.bool,
  queryFilter: PropTypes.string,

  //actions
  dispatchUpdateQueryState: PropTypes.func.isRequired,
  dispatchFetchJobsList: PropTypes.func.isRequired,
  dispatchLoadItemsForFilter: PropTypes.func,
  loadNextJobsList: PropTypes.func,
  style: PropTypes.object,
  intl: PropTypes.object.isRequired,
  dispatchSetClusterType: PropTypes.func,
  handleTabChange: PropTypes.func,
  jobIdList: PropTypes.array,
  renderButtons: PropTypes.func,
  isFromExplorePage: PropTypes.bool,
  jobsColumns: PropTypes.array,
  children: PropTypes.any,
};

function mapStateToProps(state, ownProps) {
  const { location } = ownProps;
  const jobId = location.hash && location.hash.slice(1);
  const users = getDataWithItemsForFilters(state).get("users");
  const exploreState = getExploreState(state);

  return {
    jobId,
    jobList: getJobList(state, ownProps),
    queryState: parseQueryState(location.query),
    next: state.jobs.jobs.get("next"),
    isNextJobsInProgress: state.jobs.jobs.get("isNextJobsInProgress"),
    dataFromUserFilter: users,
    dataWithItemsForFilters: getDataWithItemsForFilters(state),
    viewState: getViewState(state, JOB_PAGE_NEW_VIEW_ID),
    clusterType: state.jobs.jobs.get("clusterType"),
    admin: state.account.get("user").get("admin"),
    queryFilter: exploreState && exploreState.view.queryFilter,
  };
}

const mapDispatchToProps = {
  dispatchUpdateQueryState: updateQueryState,
  dispatchFetchJobsList: fetchJobsList,
  dispatchLoadItemsForFilter: loadItemsForFilter,
  loadNextJobsList: loadNextJobs,
  dispatchSetClusterType: setClusterType,
};

export default compose(
  connect(mapStateToProps, mapDispatchToProps),
  withRouter,
  injectIntl
)(JobListingPage);

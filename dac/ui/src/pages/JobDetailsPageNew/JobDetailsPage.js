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
import { useState, useEffect } from 'react';
import { connect } from 'react-redux';
import { withRouter } from 'react-router';
import { compose } from 'redux';
import { injectIntl } from 'react-intl';
import DocumentTitle from 'react-document-title';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import uuid from 'uuid';
import {
  loadJobDetails,
  JOB_DETAILS_VIEW_ID,
  fetchJobExecutionDetails,
  fetchJobExecutionOperatorDetails
} from 'actions/joblist/jobList';
import { showJobProfile, cancelJobAndShowNotification } from 'actions/jobs/jobs';
import { updateViewState } from 'actions/resources';
import { downloadFile } from 'sagas/downloadFile';
import { getViewState } from 'selectors/resources';
import ViewStateWrapper from 'components/ViewStateWrapper';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import jobsUtils from '@app/utils/jobsUtils';
import { renderContent } from 'dyn-load/utils/jobsUtils';
import SideNav from '@app/components/SideNav/SideNav';
import socket from '@inject/utils/socket';
import { GetIsSocketForSingleJob } from '@inject/pages/JobDetailsPageNew/utils';

import TopPanel from './components/TopPanel/TopPanel';
import './JobDetailsPage.less';

const POLL_INTERVAL = 3000;

const JobDetailsPage = (props) => {
  const isSqlContrast = localStorageUtils.getSqlThemeContrast();
  const [currentTab, setCurrentTab] = useState('Overview');
  const [isContrast, setIsContrast] = useState(isSqlContrast);
  const [jobDetails, setJobDetails] = useState(Immutable.Map());
  const [pollId, setPollId] = useState(null);
  const [isListeningForProgress, setIsListeningForProgress] = useState(false);

  const {
    intl: {
      formatMessage
    },
    router,
    location,
    jobId,
    downloadJobFile,
    viewState,
    getJobDetails,
    showJobIdProfile,
    cancelJob,
    getViewStateDetails,
    jobDetailsFromStore,
    getJobExecutionDetails,
    jobExecutionDetails,
    getJobExecutionOperatorDetails,
    jobExecutionOperatorDetails
  } = props;

  const propsForRenderContent = {
    jobDetails,
    downloadJobFile,
    isContrast,
    setIsContrast,
    jobDetailsFromStore,
    showJobIdProfile,
    jobId,
    jobExecutionDetails,
    getJobExecutionOperatorDetails,
    jobExecutionOperatorDetails,
    location
  };

  // TODO: Revisit this to fetch the info from socket instead of making multiple calls to get job details
  useEffect(() => {
    if (GetIsSocketForSingleJob()) {
      const { query: { attempts = 1 } = {} } = location || {};

      const skipStartAction =
        jobDetails && jobDetails.size !== 0 && jobDetails.get('id') === jobId;
      fetchJobDetails(skipStartAction);
      const jobAttempt = jobDetailsFromStore
        ? jobDetailsFromStore.get('totalAttempts')
        : attempts;
      router.replace({
        ...location,
        query: {
          attempts: jobAttempt
        }
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
          const { query: { attempts = 1 } = {} } = location || {};
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
            ...location,
            query: {
              attempts: jobAttempts
            }
          });
          if (!jobsUtils.isJobRunning(response && response.jobStatus)) {
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

  const fetchJobDetails = async (skipStartAction) => {
    const { query: { attempts = 1 } = {} } = location || {};
    // Skip start action for updates to the same job to avoid screen flickering (due to the spinner)
    const response = await getJobDetails(
      jobId,
      JOB_DETAILS_VIEW_ID,
      attempts,
      skipStartAction
    );
    if (!response) return; // no-payload error

    if (!response.error) {
      if (
        GetIsSocketForSingleJob() &&
        (jobDetails.size === 0 || jobDetails.get('id') !== jobId) &&
        jobsUtils.isJobRunning(response.jobStatus)
      ) {
        socket.startListenToQVJobProgress(jobId);
        setIsListeningForProgress(true);
      }
      setJobDetails(Immutable.fromJS(response));
    } else if (response.status === 404) {
      const errorMessage = formatMessage({ id: 'Job.Details.NoData' });
      getViewStateDetails(JOB_DETAILS_VIEW_ID, {
        isFailed: false,
        isWarning: true,
        isInProgress: false,
        error: {
          message: errorMessage,
          id: uuid.v4()
        }
      });
    }
    getJobExecutionDetails(
      jobId,
      JOB_DETAILS_VIEW_ID,
      attempts,
      skipStartAction
    );
    return response;
  };

  const jobStatus = jobDetailsFromStore && GetIsSocketForSingleJob() ?
    jobDetailsFromStore.get('state') :
    jobDetails.get('jobStatus');

  const breadcrumbRouting = () => {
    const { state: { history } } = location;
    router.push({
      ...history
    });
  };

  return (
    <div style={{height: '100%'}}>
      <DocumentTitle title={formatMessage({ id: 'Job.Jobs' })} />
      <div className={'jobsPageBody'}>
        <SideNav />
        <div className={'jobPageContentDiv'}>
          <ViewStateWrapper hideChildrenWhenFailed={false} viewState={viewState}>
            {
              jobDetails.get('id') &&  <div className='jobDetails'>
                <DocumentTitle title={formatMessage({ id: 'Job.JobDetails' })} />
                <div className='jobDetails__topPanel'>
                  <TopPanel
                    jobId={jobDetails.get('id')}
                    breadcrumbRouting={breadcrumbRouting}
                    setComponent={setCurrentTab}
                    jobStatus={ jobStatus}
                    jobDetails={jobDetails}
                    showJobProfile={showJobIdProfile}
                    cancelJob={cancelJob}
                  />
                </div>
                <div className='gutter-left--double gutter-right--double full-height jobDetails__bottomPanel'>
                  {renderContent(currentTab, propsForRenderContent)}
                </div>
              </div>
            }
          </ViewStateWrapper>
        </div>
      </div>
    </div>
  );
};

JobDetailsPage.propTypes = {
  intl: PropTypes.object.isRequired,
  downloadFile: PropTypes.func,
  viewState: PropTypes.instanceOf(Immutable.Map),
  jobId: PropTypes.string,
  getJobDetails: PropTypes.func,
  showJobIdProfile: PropTypes.func,
  getViewStateDetails: PropTypes.func,
  downloadJobFile: PropTypes.func,
  cancelJob: PropTypes.func,
  jobDetailsFromStore: PropTypes.object,
  router: PropTypes.object.isRequired,
  location: PropTypes.object.isRequired,
  getJobExecutionDetails: PropTypes.func,
  getJobExecutionOperatorDetails: PropTypes.func,
  jobExecutionDetails: PropTypes.any,
  jobExecutionOperatorDetails: PropTypes.any
};


function mapStateToProps(state, ownProps) {
  const {
    routeParams: {
      jobId
    } = {}
  } = ownProps;

  const jobsList = state.jobs.jobs.get('jobList').toArray();
  const currentJob = jobsList.find((job) => {
    return job.get('id') === jobId;
  });
  const totalAttempts = currentJob ? currentJob.get('totalAttempts') : undefined;
  return {
    jobId,
    totalAttempts,
    jobDetailsFromStore: currentJob,
    viewState: getViewState(state, JOB_DETAILS_VIEW_ID),
    jobExecutionDetails: state.jobs.jobs.get('jobExecutionDetails'),
    jobExecutionOperatorDetails: state.jobs.jobs.get('jobExecutionOperatorDetails')
  };
}

const mapDispatchToProps = {
  getJobDetails: loadJobDetails,
  showJobIdProfile: showJobProfile,
  getViewStateDetails: updateViewState,
  cancelJob: cancelJobAndShowNotification,
  downloadJobFile: downloadFile,
  getJobExecutionDetails: fetchJobExecutionDetails,
  getJobExecutionOperatorDetails: fetchJobExecutionOperatorDetails
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps),
  injectIntl
)(JobDetailsPage);

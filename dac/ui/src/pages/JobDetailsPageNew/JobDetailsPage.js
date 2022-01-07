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
import { loadJobDetails, JOB_DETAILS_VIEW_ID } from 'actions/joblist/jobList';
import { showJobProfile, cancelJobAndShowNotification } from 'actions/jobs/jobs';
import { updateViewState } from 'actions/resources';
import { downloadFile } from 'sagas/downloadFile';
import { getViewState } from 'selectors/resources';
import ViewStateWrapper from 'components/ViewStateWrapper';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import jobsUtils from '@app/utils/jobsUtils';
import SideNav from '@app/components/SideNav/SideNav';
import socket from '@inject/utils/socket';

import TopPanel from './components/TopPanel/TopPanel';
import OverView from './components/OverView/OverView';
import SQL from './components/SQLTab/SQLTab';
import Profile from './components/Profile/Profile';
import './JobDetailsPage.less';

const JobDetailsPage = (props) => {
  const isSqlContrast = localStorageUtils.getSqlThemeContrast();
  const [currentTab, setCurrentTab] = useState('Overview');
  const [isContrast, setIsContrast] = useState(isSqlContrast);
  const [jobDetails, setJobDetails] = useState(Immutable.Map());
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
    totalAttempts,
    getViewStateDetails,
    jobDetailsFromStore
  } = props;

  // TODO: Revisit this to fetch the info from socket instead of making multiple calls to get job details
  useEffect(() => {
    // Skip start action for updates to the same job to avoid screen flickering (due to the spinner)
    const skipStartAction = jobDetails && jobDetails.size !== 0 && jobDetails.get('id') === jobId;
    getJobDetails(jobId, JOB_DETAILS_VIEW_ID, totalAttempts, skipStartAction)
      .then((response) => {
        if (!response) return; // no-payload error

        if (!response.error) {
          if (
            (jobDetails.size === 0 || jobDetails.get('id') !== jobId) &&
            jobsUtils.isJobRunning(response.jobStatus)
          ) {
            socket.startListenToQVJobProgress(jobId);
            setIsListeningForProgress(true);
          }
          setJobDetails(Immutable.fromJS(response));
        } else if (response.status === 404) {
          const errorMessage = formatMessage({ id: 'Job.Details.NoData'});
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
      });
  }, [jobId, jobDetailsFromStore]);

  useEffect(() => {
    if (isListeningForProgress) {
      return () => socket.stoptListenToQVJobProgress(jobId);
    }
  }, [isListeningForProgress]);

  const breadcrumbRouting = () => {
    const { state: { history } } = location;
    router.push({
      ...history
    });
  };

  const renderContent = (contentPage) => {
    switch (contentPage) {
    case 'Overview':
      return <OverView
        sql={jobDetails.get('queryText')}
        jobDetails={jobDetails}
        downloadJobFile={downloadJobFile}
        isContrast = {isContrast}
        onClick = {setIsContrast}
        status={jobDetailsFromStore ? jobDetailsFromStore.get('state') : jobDetails.get('jobStatus')}
        location={location}
      />;
    case 'SQL':
      return <SQL
        submittedSql={jobDetails.get('queryText')}
        datasetGraph={jobDetails.get('datasetGraph')}
        algebricMatch={jobDetails.get('algebraicReflectionsDataset')}
        isContrast = {isContrast}
        onClick = {setIsContrast}
      />;
    case 'Profile':
      return <Profile jobDetails={jobDetails} showJobProfile={showJobIdProfile}/>;
    default:
      return <OverView sql={jobDetails.get('queryText')} status={jobDetailsFromStore ? jobDetailsFromStore.get('state') : jobDetails.get('jobStatus')} />;
    }
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
                    jobStatus={ jobDetailsFromStore ? jobDetailsFromStore.get('state') : jobDetails.get('jobStatus')}
                    jobDetails={jobDetails}
                    showJobProfile={showJobIdProfile}
                    cancelJob={cancelJob}
                  />
                </div>
                <div className='gutter-left--double full-height'>
                  {renderContent(currentTab)}
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
  totalAttempts: PropTypes.number,
  getViewStateDetails: PropTypes.func,
  downloadJobFile: PropTypes.func,
  cancelJob: PropTypes.func,
  jobDetailsFromStore: PropTypes.object,
  router: PropTypes.object.isRequired,
  location: PropTypes.object.isRequired
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
    viewState: getViewState(state, JOB_DETAILS_VIEW_ID)
  };
}

const mapDispatchToProps = {
  getJobDetails: loadJobDetails,
  showJobIdProfile: showJobProfile,
  getViewStateDetails: updateViewState,
  cancelJob: cancelJobAndShowNotification,
  downloadJobFile: downloadFile
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps),
  injectIntl
)(JobDetailsPage);

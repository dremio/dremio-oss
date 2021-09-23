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
import { compose } from 'redux';
import { injectIntl } from 'react-intl';
import DocumentTitle from 'react-document-title';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import uuid from 'uuid';
import { loadJobDetails, JOB_DETAILS_VIEW_ID } from 'actions/joblist/jobList';
import { showJobProfile, cancelJobAndShowNotification } from 'actions/jobs/jobs';
import { updateViewState } from 'actions/resources';
import { getViewState } from 'selectors/resources';
import ViewStateWrapper from 'components/ViewStateWrapper';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
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
  const {
    intl: {
      formatMessage
    },
    jobId,
    downloadFile,
    viewState,
    getJobDetails,
    showJobIdProfile,
    cancelJob,
    totalAttempts,
    getViewStateDetails
  } = props;

  useEffect(() => {
    getJobDetails(jobId, JOB_DETAILS_VIEW_ID, totalAttempts)
      .then((response) => {
        if (!response) return; // no-payload error

        if (!response.error) {
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
  }, [jobId]);

  const renderContent = (contentPage) => {
    switch (contentPage) {
    case 'Overview':
      return <OverView
        sql={jobDetails.get('queryText')}
        jobDetails={jobDetails}
        downloadJobFile={downloadFile}
        isContrast = {isContrast}
        onClick = {setIsContrast}
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
      return <OverView sql={jobDetails.get('queryText')} />;
    }
  };

  return (
    <ViewStateWrapper hideChildrenWhenFailed={false} viewState={viewState}>
      {
        jobDetails.get('id') &&  <div className='jobDetails'>
          <DocumentTitle title={formatMessage({ id: 'Job.JobDetails' })} />
          <div className='jobDetails__topPanel'>
            <TopPanel
              jobId={jobDetails.get('id')}
              changePages={props.changePages}
              setComponent={setCurrentTab}
              jobStatus={jobDetails.get('jobStatus')}
              jobDetails={jobDetails}
              showJobProfile={showJobIdProfile}
              cancelJob={cancelJob}
            />
          </div>
          <div className='gutter-left--double'>
            {renderContent(currentTab)}
          </div>
        </div>
      }
    </ViewStateWrapper>
  );
};

JobDetailsPage.propTypes = {
  changePages: PropTypes.func,
  intl: PropTypes.object.isRequired,
  downloadFile: PropTypes.func,
  viewState: PropTypes.instanceOf(Immutable.Map),
  jobId: PropTypes.string,
  getJobDetails: PropTypes.func,
  showJobIdProfile: PropTypes.func,
  totalAttempts: PropTypes.number,
  getViewStateDetails: PropTypes.func,
  cancelJob: PropTypes.func
};


function mapStateToProps(state) {
  return {
    viewState: getViewState(state, JOB_DETAILS_VIEW_ID)
  };
}

const mapDispatchToProps = dispatch => ({
  getJobDetails: (jobId, viewId, totalAttempts) => dispatch(loadJobDetails(jobId, viewId, totalAttempts)),
  showJobIdProfile: (profileUrl) => dispatch(showJobProfile(profileUrl)),
  getViewStateDetails:(viewId, errObj) => dispatch(updateViewState(viewId, errObj)),
  cancelJob: (jobId) => dispatch(cancelJobAndShowNotification(jobId))
});

export default compose(
  connect(mapStateToProps, mapDispatchToProps),
  injectIntl
)(JobDetailsPage);

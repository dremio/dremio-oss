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
import { useState, useRef, useEffect } from 'react';
import { connect } from 'react-redux';
import { compose } from 'redux';
import Immutable from 'immutable';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';
import { flexElementAuto } from '@app/uiTheme/less/layout.less';
import { getClusterInfo } from '@app/utils/infoUtils';
import { getSupport } from '@app/utils/supportUtils';

import { updateQueryState, setClusterType } from 'actions/jobs/jobs';
import {
  fetchJobsList,
  loadItemsForFilter,
  loadNextJobs,
  JOB_PAGE_NEW_VIEW_ID
} from 'actions/joblist/jobList';
import { downloadFile } from 'sagas/downloadFile';
import { getJobList, getDataWithItemsForFilters } from 'selectors/jobs';
import { getViewState } from 'selectors/resources';
import { isEqual, isEmpty } from 'lodash';

import { parseQueryState } from 'utils/jobsQueryState';
import MainHeader from 'components/MainHeader';

import JobDetailsPage from '../JobDetailsPageNew/JobDetailsPage';
import JobsContent from './components/JobsContent';
import './JobPageNew.less';

const JobPage = (props) => {
  const usePrevious = (value) => {
    const ref = useRef();
    useEffect(() => {
      ref.current = value;
    });
    return ref.current;
  };

  const {
    queryState, viewState, style, location, intl, next,
    isNextJobsInProgress, dataWithItemsForFilters, admin,
    jobList, downloadJobFile, loadNextJobsList
  } = props;
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [lastLoaded, setLastLoaded] = useState('');
  const [previousJobId, setPreviousJobId] = useState('');
  const prevQueryState = usePrevious(queryState);

  useEffect(() => {
    props.updateQueryState(queryState.setIn(['filters', 'qt'], ['UI', 'EXTERNAL']));
    handleCluster();
  }, []);

  useEffect(() => {
    if ((!isEmpty(location.query)) && !isEqual(queryState, prevQueryState)) {
      props.fetchJobsList(queryState, JOB_PAGE_NEW_VIEW_ID);
    }
  }, [queryState]);

  useEffect(() => {
    if (isEmpty(location.query)) {
      if (!queryState.equals(prevQueryState)) {
        props.updateQueryState(queryState.setIn(['filters', 'qt'], ['UI', 'EXTERNAL']));
      }
      setSelectedJobId(null);
    }
  }, [location]);

  const handleCluster = async () => {
    const clusterInfo = await getClusterInfo();
    const supportInfo = getSupport(admin) !== undefined ? getSupport(admin) : false;
    const data = {
      clusterType: clusterInfo.clusterType,
      isSupport: supportInfo
    };
    clusterInfo.clusterType !== undefined ? props.setClusterType(data) : props.setClusterType('NP');
  };

  const changePages = (data) => {
    if (data !== null) {
      const currentJobId = data.rowData.data.job.value;
      setSelectedJobId(currentJobId);
    } else {
      setSelectedJobId(null);
    }
  };
  let recentJobId = '';
  const tableRowRenderer = (index) => {
    const lastJob = jobList.get(index);
    const lastJobId = lastJob && lastJob.get('id');
    if (index + 1 === jobList.size && next &&
      lastLoaded !== next && recentJobId !== lastJobId) {
      loadNextJobsList(next, JOB_PAGE_NEW_VIEW_ID);
      setLastLoaded(next);
      lastJob && setPreviousJobId(lastJobId);
      recentJobId = lastJobId;
    }
  };

  const currentJob = jobList.find(job => job.get('id') === selectedJobId);
  const totalAttempts = currentJob ? currentJob.get('totalAttempts') : 1;
  return (
    <div style={style}>
      <DocumentTitle title={intl.formatMessage({ id: 'Job.Jobs' })} />
      <MainHeader />
      {selectedJobId ?
        <JobDetailsPage
          jobId={selectedJobId}
          totalAttempts={totalAttempts}
          changePages={changePages}
          downloadFile={downloadJobFile}
        />
        :
        <div className='jobPageNew'>
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
            onUpdateQueryState={props.updateQueryState}
            loadItemsForFilter={props.loadItemsForFilter}
            dataWithItemsForFilters={dataWithItemsForFilters}
            changePages={changePages}
          />
        </div>
      }
    </div>
  );
};

JobPage.propTypes = {
  location: PropTypes.object.isRequired,
  jobId: PropTypes.string,
  jobList: PropTypes.instanceOf(Immutable.List).isRequired,
  queryState: PropTypes.instanceOf(Immutable.Map).isRequired,
  next: PropTypes.string,
  viewState: PropTypes.instanceOf(Immutable.Map),
  isNextJobsInProgress: PropTypes.bool,
  dataWithItemsForFilters: PropTypes.object,
  clusterType: PropTypes.string,
  admin: PropTypes.bool,


  //actions
  updateQueryState: PropTypes.func.isRequired,
  fetchJobsList: PropTypes.func.isRequired,
  loadItemsForFilter: PropTypes.func,
  loadNextJobsList: PropTypes.func,
  style: PropTypes.object,
  intl: PropTypes.object.isRequired,
  setClusterType: PropTypes.func,
  downloadJobFile: PropTypes.func
};

function mapStateToProps(state, ownProps) {
  const { location } = ownProps;
  const jobId = location.hash && location.hash.slice(1);
  return {
    jobId,
    jobList: getJobList(state, ownProps),
    queryState: parseQueryState(location.query),
    next: state.jobs.jobs.get('next'),
    isNextJobsInProgress: state.jobs.jobs.get('isNextJobsInProgress'),
    dataWithItemsForFilters: getDataWithItemsForFilters(state),
    viewState: getViewState(state, JOB_PAGE_NEW_VIEW_ID),
    clusterType: state.jobs.jobs.get('clusterType'),
    admin: state.account.get('user').get('admin')
  };
}

const mapDispatchToProps = dispatch => ({
  updateQueryState: (queryState) => dispatch(updateQueryState(queryState)),
  fetchJobsList: (queryState, viewId) => dispatch(fetchJobsList(queryState, viewId)),
  loadItemsForFilter: (tag, item, limit) => dispatch(loadItemsForFilter(tag, item, limit)),
  loadNextJobsList: (href, viewId) => dispatch(loadNextJobs(href, viewId)),
  setClusterType: (type) => dispatch(setClusterType(type)),
  downloadJobFile: (data) => dispatch(downloadFile(data))
});

export default compose(
  connect(mapStateToProps, mapDispatchToProps),
  injectIntl
)(JobPage);

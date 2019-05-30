/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import { connect }   from 'react-redux';
import Immutable  from 'immutable';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';
import { flexElementAuto } from '@app/uiTheme/less/layout.less';

import {
  updateQueryState, filterJobsData, loadItemsForFilter, loadNextJobs
} from 'actions/jobs/jobs';

import { getJobs, getDataWithItemsForFilters } from 'selectors/jobs';
import { getViewState } from 'selectors/resources';

import { parseQueryState } from 'utils/jobsQueryState';
import jobsUtils from 'utils/jobsUtils';
import MainHeader from 'components/MainHeader';

import RunningJobsHeader from './components/RunningJobsHeader';
import JobsContent from './components/JobsContent';

const VIEW_ID = 'JOB_PAGE_VIEW_ID';

@injectIntl
@PureRender
export class JobPage extends Component {
  static propTypes = {
    location: PropTypes.object.isRequired,
    jobId: PropTypes.string,
    jobs: PropTypes.instanceOf(Immutable.List).isRequired,
    queryState: PropTypes.instanceOf(Immutable.Map).isRequired,
    next: PropTypes.string,
    viewState: PropTypes.instanceOf(Immutable.Map),
    isNextJobsInProgress: PropTypes.bool,
    dataWithItemsForFilters: PropTypes.object,

    //actions
    updateQueryState: PropTypes.func.isRequired,
    filterJobsData: PropTypes.func.isRequired,
    loadItemsForFilter: PropTypes.func,
    loadNextJobs: PropTypes.func,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  componentDidMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, prevProps = {}) {
    const { queryState } = nextProps;
    if (!Object.keys(nextProps.location.query).length) { // first load, or re-clicking "Jobs" in the header
      nextProps.updateQueryState(queryState.setIn(['filters', 'qt'], ['UI', 'EXTERNAL']));
    } else if (!nextProps.queryState.equals(prevProps.queryState)) {
      nextProps.filterJobsData(nextProps.queryState, VIEW_ID);
    }
  }

  render() {
    const { jobId, jobs, queryState, viewState, style, location, intl } = this.props;
    const runningJobsCount = jobsUtils.getNumberOfRunningJobs(jobs);

    return (
      <div style={style}>
        <DocumentTitle title={intl.formatMessage({ id: 'Job.Jobs' })} />
        <MainHeader />
        <RunningJobsHeader jobCount={runningJobsCount}/>
        <JobsContent
          className={flexElementAuto} // Page object adds flex in style
          loadNextJobs={this.props.loadNextJobs}
          // todo: update to react-router v3 so don't have to deep pass `location` anymore
          location={location}
          jobId={jobId}
          jobs={jobs}
          queryState={queryState}
          next={this.props.next}
          isNextJobsInProgress={this.props.isNextJobsInProgress}
          viewState={viewState}
          onUpdateQueryState={this.props.updateQueryState}
          loadItemsForFilter={this.props.loadItemsForFilter}
          dataWithItemsForFilters={this.props.dataWithItemsForFilters}
        />
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { location } = ownProps;
  const jobId = location.hash && location.hash.slice(1);
  return {
    jobId,
    jobs: getJobs(state, ownProps),
    queryState: parseQueryState(location.query),
    next: state.jobs.jobs.get('next'),
    isNextJobsInProgress: state.jobs.jobs.get('isNextJobsInProgress'),
    dataWithItemsForFilters: getDataWithItemsForFilters(state),
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  updateQueryState,
  filterJobsData,
  loadItemsForFilter,
  loadNextJobs
})(JobPage);

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
import { PureComponent } from 'react';
import $ from 'jquery';
import classNames from 'classnames';
import Immutable  from 'immutable';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import socket from 'utils/socket';
import { flexColumnContainer } from '@app/uiTheme/less/layout.less';

import ViewStateWrapper from 'components/ViewStateWrapper';
import ViewCheckContent from 'components/ViewCheckContent';
import JobTable from './JobsTable/JobTable';
import JobDetails from './JobDetails/JobDetails';
import JobsFilters from './JobsFilters/JobsFilters';

// export this for calculate min width of table tr in JobTable.js
export const SEPARATOR_WIDTH = 10;
export const MIN_LEFT_PANEL_WIDTH = 500;
const MIN_RIGHT_PANEL_WIDTH = 330;

@injectIntl
@Radium
export default class JobsContent extends PureComponent {

  static propTypes = {
    jobId: PropTypes.string,
    jobs: PropTypes.instanceOf(Immutable.List).isRequired,
    queryState: PropTypes.instanceOf(Immutable.Map).isRequired,
    next: PropTypes.string,
    onUpdateQueryState: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map),
    dataWithItemsForFilters: PropTypes.object,
    isNextJobsInProgress: PropTypes.bool,
    location: PropTypes.object,
    intl: PropTypes.object.isRequired,
    className: PropTypes.string,

    loadItemsForFilter: PropTypes.func,
    loadNextJobs: PropTypes.func
  };

  static defaultProps = {
    jobs: Immutable.List()
  }

  static contextTypes = {
    router: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.handleResizeJobs = this.handleResizeJobs.bind(this);
    this.getActiveJob = this.getActiveJob.bind(this);
    this.handleMouseReleaseOutOfBrowser = this.handleMouseReleaseOutOfBrowser.bind(this);
    this.state = {
      isResizing: false,
      width: 'calc(50% - 22px)',
      left: 'calc(50% - 22px)',
      curId: ''
    };
  }

  componentDidMount() {
    $(window).on('mouseup', this.handleMouseReleaseOutOfBrowser);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.jobs !== this.props.jobs) {
      this.runActionForJobs(nextProps.jobs, false, (jobId) => socket.startListenToJobProgress(jobId));

      // if we don't have an active job id highlight the first job
      if (!nextProps.jobId) {
        this.setActiveJob(nextProps.jobs.get(0), true);
      }
    }
  }

  componentWillUnmount() {
    $(window).off('mouseup', this.handleMouseReleaseOutOfBrowser);
    this.runActionForJobs(this.props.jobs, true, (jobId) => socket.stopListenToJobProgress(jobId));
  }

  getActiveJob() {
    const { jobId } = this.props;
    if (jobId) {
      return this.findCurrentJob(this.props, jobId);
    }
  }

  setActiveJob = (jobData, isReplaceUrl) => {
    const { location } = this.props;
    const router = this.context.router[isReplaceUrl ? 'replace' : 'push'];
    if (jobData) {
      router({...location, hash: `#${jobData.get('id')}`});
    } else {
      router({...location, hash: ''});
    }
  };

  findCurrentJob(props, jobId) {
    return props.jobs.size && props.jobs.find((item) => item.get('id') === jobId);
  }

  runActionForJobs(jobs, isStop, callback) {
    jobs.forEach((job) => {
      const jobId = job.get('id');
      const jobState = job.get('state');
      if (isStop || ['NOT_SUBMITTED', 'STARTING', 'RUNNING', 'ENQUEUED', 'CANCELLATION_REQUESTED'].includes(jobState)) {
        return callback(jobId);
      }
    });
  }

  handleMouseReleaseOutOfBrowser() {
    if (this.state.isResizing) {
      this.handleEndResize();
    }
  }

  handleStartResize = () => this.setState({ isResizing: true })

  handleEndResize = () => {
    this.setState({
      isResizing: false,
      width: typeof this.state.left === 'number' ? this.state.left - SEPARATOR_WIDTH : this.state.width });
  }

  handleResizeJobs(e) {
    const left = e && (e.clientX - SEPARATOR_WIDTH / 2);
    if (this.state.isResizing && left > MIN_LEFT_PANEL_WIDTH) {
      const width = document.body.offsetWidth - left;
      if ( width > MIN_RIGHT_PANEL_WIDTH) {
        this.setState({
          left
        });
      }
    }
  }

  render() {
    const {
      jobId, jobs, queryState, onUpdateQueryState,
      viewState, location, intl, className
    } = this.props;
    const query = location.query || {};
    const resizeStyle = this.state.isResizing ? styles.noSelection : {};

    return (
      <div className={classNames('jobs-content', flexColumnContainer, className)} style={[styles.base, resizeStyle]} ref='content'>
        <JobsFilters
          queryState={queryState}
          onUpdateQueryState={onUpdateQueryState}
          style={styles.filters}
          loadItemsForFilter={this.props.loadItemsForFilter}
          dataWithItemsForFilters={this.props.dataWithItemsForFilters} />
        <ViewStateWrapper viewState={viewState} style={styles.viewState}>
          <ViewCheckContent
            viewState={viewState}
            message={intl.formatMessage({ id: 'Job.NoMatchingJobsFound' })}
            dataIsNotAvailable={!jobs.size}
          >
            <div className='job-wrapper' style={styles.jobWrapper}
              onMouseMove={this.handleResizeJobs}
              onMouseUp={this.handleEndResize}>
              <JobTable
                isNextJobsInProgress={this.props.isNextJobsInProgress}
                loadNextJobs={this.props.loadNextJobs}
                jobs={jobs}
                next={this.props.next}
                width={this.state.width}
                viewState={viewState}
                setActiveJob={this.setActiveJob}
                isResizing={this.state.isResizing}
                containsTextValue={query.contains ? query.contains : ''}
                jobId={jobId}
              />
              <div className='separator'
                style={[styles.separator, {left: this.state.left}]}
                onMouseDown={this.handleStartResize}>
              </div>

              <JobDetails
                ref='jobDetails'
                jobId={jobId}
                location={this.props.location}
              />
            </div>
          </ViewCheckContent>
        </ViewStateWrapper>
      </div>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    flex: 1,
    display: 'flex',
    flexDirection: 'column',
    width: '100%'
  },
  viewState: {
    display: 'flex',
    // this is needed to force a view state wrapper fit to parent and do not overflow it
    // as this cause a scrollbar to appear
    minHeight: 0
  },
  filters: {
    flexShrink: 0
  },
  jobWrapper: {
    flex: 1,
    position: 'relative',
    overflow: 'hidden',
    display: 'flex',
    backgroundColor: '#ccc',
    padding: '10px'
  },
  separator: {
    width: 10,
    background: '#ccc',
    cursor: 'col-resize',
    position: 'absolute',
    top: 0,
    bottom: 0,
    zIndex: 999,
    left: '50%',
    opacity: '.6'
  },
  noSelection: {
    userSelect: 'none'
  }
};

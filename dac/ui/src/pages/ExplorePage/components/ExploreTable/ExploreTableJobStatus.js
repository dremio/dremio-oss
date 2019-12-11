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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { withRouter } from 'react-router';

import DropdownMenu from '@app/components/Menus/DropdownMenu';
import RealTimeTimer from '@app/components/RealTimeTimer';
import { JobStatusMenu } from '@app/components/Menus/ExplorePage/JobStatusMenu';
import SampleDataMessage from '@app/pages/ExplorePage/components/SampleDataMessage';
import ExploreTableJobStatusSpinner from '@app/pages/ExplorePage/components/ExploreTable/ExploreTableJobStatusSpinner';
import jobsUtils from '@app/utils/jobsUtils';
import {getJobProgress, getImmutableTable, getExploreJobId, getJobOutputRecords} from '@app/selectors/explore';
import { cancelJobAndShowNotification } from '@app/actions/jobs/jobs';



export const JOB_STATUS = {
  notSubmitted: 'NOT_SUBMITTED',
  starting: 'STARTING',
  running: 'RUNNING',
  completed: 'COMPLETED',
  canceled: 'CANCELED',
  failed: 'FAILED',
  cancellationRequested: 'CANCELLATION_REQUESTED',
  enqueued: 'ENQUEUED'
};

export const isWorking = (status) => {
  return [
    JOB_STATUS.starting,
    JOB_STATUS.enqueued,
    JOB_STATUS.running,
    JOB_STATUS.cancellationRequested].includes(status);
};

export class ExploreTableJobStatus extends Component {
  static propTypes = {
    approximate: PropTypes.bool,
    //connected
    jobProgress: PropTypes.object,
    jobId: PropTypes.string,
    haveRows: PropTypes.bool,
    outputRecords: PropTypes.number,
    cancelJob: PropTypes.func,
    //withRouter props
    location: PropTypes.object.isRequired
  };

  jobStatusNames = {
    [JOB_STATUS.notSubmitted]: la('Not Submitted'),
    [JOB_STATUS.starting]: la('Starting'),
    [JOB_STATUS.running]: la('Running'),
    [JOB_STATUS.completed]: la('Completed'),
    [JOB_STATUS.canceled]: la('Canceled'),
    [JOB_STATUS.failed]: la('Failed'),
    [JOB_STATUS.cancellationRequested]: la('Cancellation Requested'),
    [JOB_STATUS.enqueued]: la('Enqueued')
  };

  doButtonAction = (actionType) => {
    const {cancelJob, jobProgress: {jobId}} = this.props;
    if (!jobId) return;

    if (actionType === 'cancel') {
      cancelJob(jobId);
    } //else ignore
  };

  renderTime = jobProgress => {
    // if not complete - show timer, else format end-start
    const { startTime, endTime } = jobProgress;
    if (isWorking(jobProgress.status)) {
      return (
        <RealTimeTimer
          startTime={startTime}
          formatter={jobsUtils.formatJobDuration}/>
      );
    } else if (startTime && endTime) {
      return jobsUtils.formatJobDuration(endTime - startTime);
    } else {
      return null;
    }
  };

  renderPreviewWarning = () => {
    //in case there was no jobProgress, show "preview" warning once table data appears
    const { approximate, haveRows } = this.props;
    if (approximate && haveRows) {
      return <SampleDataMessage />;
    }
    return null;
  };

  getCancellable = jobStatus => {
    return jobStatus === JOB_STATUS.running
      || jobStatus === JOB_STATUS.starting
      || jobStatus === JOB_STATUS.enqueued;
  };

  render() {
    const { jobProgress, jobId, outputRecords } = this.props;
    if (!jobProgress) {
      return this.renderPreviewWarning();
    }

    const jobTypeLabel = jobProgress.isRun ? la('Run') : la('Preview');
    const isCompleteWithRecords = jobProgress.status === JOB_STATUS.completed && outputRecords;
    const jobStatusLabel = (isCompleteWithRecords) ? la('Records: ') : la('Status: ');
    const jobStatusName = (isCompleteWithRecords) ? outputRecords.toLocaleString() : this.jobStatusNames[jobProgress.status];
    const isJobCancellable = this.getCancellable(jobProgress.status);

    return (
      <div style={styles.wrapper}>
        <span style={styles.label}>{la('Job: ')}</span>
        <span style={styles.value}>{jobTypeLabel}</span>
        <span style={styles.divider}> | </span>
        <span style={styles.label}>{jobStatusLabel}</span>
        <span style={styles.value}>
          {!jobId && <span style={styles.text}>{jobStatusName}</span>}
          {jobId &&
          <DropdownMenu
            className='explore-job-status-button'
            hideArrow
            hideDivider
            style={styles.textLink}
            text={jobStatusName}
            menu={<JobStatusMenu action={this.doButtonAction} jobId={jobId} isCancellable={isJobCancellable}/>}/>
          }
          <ExploreTableJobStatusSpinner jobProgress={jobProgress} jobId={jobId}/>
        </span>
        <span style={styles.divider}> | </span>
        <span style={styles.label}>{la('Time: ')}</span>
        <span style={styles.value}>
          {this.renderTime(jobProgress)}
        </span>
      </div>
    );
  }

}

function mapStateToProps(state, props) {
  const jobProgress = getJobProgress(state);
  const jobId = getExploreJobId(state);
  const outputRecords = getJobOutputRecords(state);
  const {approximate, location = {}} = props;

  let haveRows = false;
  // get preview tableData for preview w/o jobProgress
  if (!jobProgress && approximate) {
    const version = location.query && location.query.version;
    const tableData = getImmutableTable(state, version);
    const rows = tableData.get('rows');
    haveRows = rows && !!rows.size;
  }

  return {
    jobProgress,
    jobId,
    haveRows,
    outputRecords
  };
}

export default withRouter(connect(mapStateToProps, {
  cancelJob: cancelJobAndShowNotification
})(ExploreTableJobStatus));

const styles = {
  wrapper: {
    display: 'flex',
    alignItems: 'center'
  },
  label: {
    display: 'inline-box',
    paddingRight: 3,
    fontWeight: 500
  },
  value: {
    display: 'inline-flex',
    alignItems: 'center'
  },
  divider: {
    display: 'inline-box',
    padding: '7px 5px',
    fontWeight: 500
  },
  textLink: {
    color: '#43B8C9',
    marginRight: 0
  },
  text: {
    marginRight: 6
  }
};

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
import { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { withRouter, Link } from 'react-router';
import { injectIntl } from 'react-intl';

import { addNotification } from '@app/actions/notification';
import RealTimeTimer from '@app/components/RealTimeTimer';
import SampleDataMessage from '@app/pages/ExplorePage/components/SampleDataMessage';
import jobsUtils from '@app/utils/jobsUtils';
import {
  getJobProgress,
  getRunStatus,
  getImmutableTable,
  getExploreJobId,
  getJobOutputRecords
} from '@app/selectors/explore';
import './ExploreTableJobStatus.less';
import { compose } from 'redux';

export const JOB_STATUS = {
  notSubmitted: 'NOT_SUBMITTED',
  starting: 'STARTING',
  running: 'RUNNING',
  completed: 'COMPLETED',
  canceled: 'CANCELED',
  failed: 'FAILED',
  cancellationRequested: 'CANCELLATION_REQUESTED',
  enqueued: 'ENQUEUED',
  pending: 'PENDING',
  planning: 'PLANNING',
  metadataRetrieval: 'METADATA_RETRIEVAL',
  engineStart: 'ENGINE_START',
  queued: 'QUEUED',
  executionPlanning: 'EXECUTION_PLANNING'
};

export const isWorking = (status) => {
  return [
    JOB_STATUS.starting,
    JOB_STATUS.enqueued,
    JOB_STATUS.running,
    JOB_STATUS.cancellationRequested,
    JOB_STATUS.pending,
    JOB_STATUS.metadataRetrieval,
    JOB_STATUS.planning,
    JOB_STATUS.engineStart,
    JOB_STATUS.queued,
    JOB_STATUS.executionPlanning].includes(status);
};

const ExploreTableJobStatus = (props) => {
  const {
    jobProgress,
    jobId,
    jobAttempts,
    runStatus,
    outputRecords,
    approximate,
    haveRows,
    intl
  } = props;

  const jobStatusNames = {
    [JOB_STATUS.notSubmitted]: intl.formatMessage({ id: 'JobStatus.NotSubmitted' }),
    [JOB_STATUS.starting]: intl.formatMessage({ id: 'JobStatus.Starting' }),
    [JOB_STATUS.running]: intl.formatMessage({ id: 'JobStatus.Running' }),
    [JOB_STATUS.completed]: intl.formatMessage({ id: 'JobStatus.Completed' }),
    [JOB_STATUS.canceled]: intl.formatMessage({ id: 'JobStatus.Canceled' }),
    [JOB_STATUS.failed]: intl.formatMessage({ id: 'JobStatus.Failed' }),
    [JOB_STATUS.cancellationRequested]: intl.formatMessage({ id: 'JobStatus.CancellationRequested' }),
    [JOB_STATUS.enqueued]: intl.formatMessage({ id: 'JobStatus.Enqueued' }),
    [JOB_STATUS.pending]: intl.formatMessage({ id: 'JobStatus.Pending' }),
    [JOB_STATUS.metadataRetrieval]: intl.formatMessage({ id: 'JobStatus.MetadataRetrieval' }),
    [JOB_STATUS.planning]: intl.formatMessage({ id: 'JobStatus.Planning' }),
    [JOB_STATUS.engineStart]: intl.formatMessage({ id: 'JobStatus.EngineStart' }),
    [JOB_STATUS.queued]: intl.formatMessage({ id: 'JobStatus.Queued' }),
    [JOB_STATUS.executionPlanning]: intl.formatMessage({ id: 'JobStatus.ExecutionPlanning' })
  };

  const [ jobStatusLabel, setJobStatusLabel ] = useState('');
  const [ jobStatusName, setJobStatusName ] = useState('');
  const [ jobType, setJobType ] = useState(null);
  const [ jobProgressId, setJobProgressId ] = useState(null);

  useEffect(() => {
    if (jobProgress === null) {
      renderPreviewWarning();
      return;
    }

    setJobProgressId(jobId);
    setJobType(runStatus ? intl.formatMessage({ id: 'Explore.Run'}) : intl.formatMessage({ id: 'Explore.Preview'}));

    if (jobProgress.status === JOB_STATUS.completed) {
      setJobStatusLabel(intl.formatMessage({ id: 'Explore.Records'}));

      if (outputRecords != null) {
        setJobStatusName(outputRecords.toLocaleString());
      }
    } else {
      setJobStatusLabel(intl.formatMessage({ id: 'Explore.Status'}));
      setJobStatusName(jobStatusNames[jobProgress.status]);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobProgress]);

  const renderTime = () => {
    if (!jobProgress) return null;
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

  const renderPreviewWarning = () => {
    //in case there was no jobProgress, show "preview" warning once table data appears
    if (approximate && haveRows) {
      return <SampleDataMessage />;
    }
  };

  if (jobProgress === null) {
    return null;
  } else {
    return (
      <div className='exploreJobStatus'>
        <div className='exploreJobStatus__item'>
          <span style={styles.label}>{intl.formatMessage({ id: 'Explore.Job' })}: </span>

          {jobProgressId &&
            <Link
              to={{
                pathname: `/job/${jobId}`,
                query: {
                  attempts: jobAttempts
                },
                state: {
                  isFromJobListing: false
                }
              }}
              title={`Jobs Detail Page for #${jobProgressId}`}>
              {jobType}
            </Link>
          }
        </div>

        <div className='exploreJobStatus__item'>
          {<span style={styles.label}>{jobStatusLabel}: </span>}
          {jobStatusName}
        </div>

        <div className='exploreJobStatus__item'>
          {renderTime()}
        </div>
      </div>
    );
  }
};

ExploreTableJobStatus.propTypes = {
  approximate: PropTypes.bool,
  //connected
  jobProgress: PropTypes.object,
  runStatus: PropTypes.bool,
  jobId: PropTypes.string,
  haveRows: PropTypes.bool,
  outputRecords: PropTypes.number,
  version: PropTypes.string,
  intl: PropTypes.object.isRequired
};

function mapStateToProps(state, props) {
  const {approximate, version} = props;
  const jobProgress = getJobProgress(state, version);
  const jobId = getExploreJobId(state);

  const jobDetails = state.resources.entities.getIn(['jobDetails', jobId]);
  const jobAttempts = jobDetails && jobDetails.get('attemptDetails') && jobDetails.get('attemptDetails').size;
  const outputRecords = getJobOutputRecords(state, version);
  const runStatus = getRunStatus(state).isRun;

  let haveRows = false;
  // get preview tableData for preview w/o jobProgress
  if (!jobProgress && approximate) {
    const tableData = getImmutableTable(state, version);
    const rows = tableData.get('rows');
    haveRows = rows && !!rows.size;
  }

  return {
    haveRows,
    jobProgress,
    jobId,
    jobAttempts: jobAttempts || 1,
    outputRecords,
    runStatus
  };
}

export default compose(
  connect(mapStateToProps, { addNotification }),
  withRouter,
  injectIntl
)(ExploreTableJobStatus);

export const styles = {
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
  timeValue: {
    minWidth: 10
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
  },
  helpTooltip: {
    zIndex: 10001
  },
  helpInnerTooltip: {
    width: 240
  },
  menuText: {
    marginRight: 0
  },
  defaultInnerStyle: {
    borderRadius: 5,
    padding: 10,
    width: 300
  }
};

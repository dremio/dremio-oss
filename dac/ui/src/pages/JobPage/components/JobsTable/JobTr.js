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
import Radium from 'radium';
import Immutable from 'immutable';
import PureRender from 'pure-render-decorator';
import { formatMessage } from 'utils/locale';

import PropTypes from 'prop-types';

import TextHighlight from 'components/TextHighlight';
import jobsUtils from 'utils/jobsUtils';
import timeUtils from 'utils/timeUtils';

import { fixedWidthSmall } from 'uiTheme/radium/typography';
import { PALE_ORANGE } from 'uiTheme/radium/colors';

import Art from 'components/Art';
import JobStateIcon from '../JobStateIcon';

const DATASET_HEIGHT = 14;

@PureRender
@Radium
export default class JobTr extends Component {
  static propTypes = {
    active: PropTypes.bool.isRequired,
    even: PropTypes.bool.isRequired,
    onClick: PropTypes.func.isRequired,
    job: PropTypes.instanceOf(Immutable.Map),
    jobDataset: PropTypes.node,
    containsTextValue: PropTypes.string
  };

  static defaultProps = {
    job: Immutable.Map()
  }

  renderSqlBlock(queryType) {
    const { job, containsTextValue } = this.props;
    if (queryType !== 'run' && queryType !== 'preview') {
      return (
        <div className='sql' style={[fixedWidthSmall, styles.description]}>
          <TextHighlight text={job.get('description')} inputValue={containsTextValue}/>
        </div>
      );
    }
    return null;
  }

  render() {
    const { even, active, onClick, containsTextValue, job, jobDataset } = this.props;
    const jobState = job.get('state');
    const running = jobsUtils.getRunning(jobState);
    const jobEndTime = job.get('endTime');
    const jobStartTime = job.get('startTime');
    const jobDuration = !running
      ? jobsUtils.getJobDuration(jobStartTime, jobEndTime)
      : '-';
    const jobFinishTime = jobsUtils.getFinishTime(jobState, jobEndTime);
    const trStyles = [styles.base];
    if (even) {
      trStyles.push(styles.even);
    }
    if (active) {
      trStyles.push(styles.active);
    }

    let flame = '';
    let flameAlt = '';
    if (job.get('accelerated')) {
      flame = job.get('snowflakeAccelerated') ? 'FlameSnowflake.svg' : 'Flame.svg';
      flameAlt = job.get('snowflakeAccelerated') ? formatMessage('Job.AcceleratedHoverSnowFlake') : formatMessage('Job.AcceleratedHover');
    }

    return (
      <div
        id={job.get('id')}
        className='jobs-table-tr'
        onClick={onClick}
        style={trStyles}>
        <div style={styles.headerRow}>
          <div style={styles.td}>
            <JobStateIcon state={jobState} />
          </div>
          <div style={styles.columns}>
            <div style={{ width: 100 }}>
              {jobDataset}
            </div>
            <div className='user'>
              <TextHighlight text={job.get('user')} inputValue={containsTextValue}/>
            </div>
            <div className='startTime'>
              {timeUtils.formatTime(jobStartTime)}
            </div>
            <div style={styles.durationStyle} className='duration'>
              {jobDuration}
              {job.get('accelerated') &&
              <Art src={flame} alt={flameAlt} style={styles.smallIcon} title/>}
              {job.get('spilled') &&
              <Art src='DiskSpill.svg' alt={formatMessage('Job.SpilledHover')} style={styles.smallIcon} title/>}
            </div>
            <div style={styles.dateTimeStyle} className='endTime'>{jobFinishTime}</div>
          </div>
        </div>
        {this.renderSqlBlock(job.get('queryType'))}
      </div>);
  }
}

const styles = {
  base: {
    cursor: 'pointer',
    padding: '5px 5px 5px 8px',
    borderBottom: '1px solid rgba(0,0,0,.07)',
    minWidth: 240,
    flex: '1 1 0',
    display: 'flex',
    flexWrap: 'wrap',
    alignItems: 'center'
  },
  headerRow: {
    display: 'flex',
    width: '100%'
  },
  active: {
    backgroundColor: PALE_ORANGE
  },
  td: {
    display: 'inline-block'
  },
  description: {
    color: '#B4B4B4',
    padding: '0 0 5px 30px',
    lineHeight: '1.5em',
    height: '3em', /* height is 2x line-height, so two lines will display */
    overflow: 'hidden'
  },
  columns: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    width: '100%',
    marginLeft: 5
  },
  alignTop: {
    verticalAlign: 'middle'
  },
  datasetName: {
    height: DATASET_HEIGHT,
    overflow: 'visible',
    margin: '7px 5px 0 0'
  },
  durationStyle: {
    display: 'flex',
    alignItems: 'center',
    minWidth: 65
  },
  dateTimeStyle: {
    minWidth: 113
  },
  smallIcon: {
    paddingLeft: 5,
    height: 20
  }
};

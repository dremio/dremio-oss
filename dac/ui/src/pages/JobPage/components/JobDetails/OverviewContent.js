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
import PropTypes from 'prop-types';
import classNames from 'classnames';

import Immutable from 'immutable';
import jobsUtils from 'utils/jobsUtils';
import timeUtils from 'utils/timeUtils';
import { getIconByEntityType } from 'utils/iconUtils';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import DatasetAccelerationButton from 'dyn-load/components/Acceleration/DatasetAccelerationButton';
import SettingsBtn from 'components/Buttons/SettingsBtn';
import RealTimeTimer from 'components/RealTimeTimer';
import CopyButton from 'components/Buttons/CopyButton';
import { injectIntl, FormattedMessage } from 'react-intl';
import { flexColumnContainer, flexElementAuto } from '@app/uiTheme/less/layout.less';

import { BORDER_TABLE } from 'uiTheme/radium/colors';
import SqlEditor from 'components/SQLEditor.js';
import { getQueueName } from 'dyn-load/pages/JobPage/components/JobDetails/OverviewContentUtil';

import Quote from './Quote';
import ListItem from './ListItem';
import JobErrorLog from './JobErrorLog';
import ReflectionList from './ReflectionList';
import ReflectionBlock from './ReflectionBlock';


@injectIntl
class OverviewContent extends PureComponent {
  static checkResultOfProfile = (attemptDetails, reason = '') => {
    if (!reason) {
      return attemptDetails.find( profile => profile.get('reason') );
    }
    return attemptDetails.filter( profile => profile.get('reason').indexOf(reason) !== -1 ).size;
  };

  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  getFormatMessageIdForQueryType() {
    const { jobDetails } = this.props;

    const requestType = jobDetails.get('requestType');
    const isPrepareCreate = requestType === 'CREATE_PREPARE';
    const isPrepareExecute = requestType === 'EXECUTE_PREPARE';
    const isMetadata = this.isMetadataJob();

    switch (jobDetails.get('queryType')) {
    case 'UI_RUN':
      return 'Job.UIRun';
    case 'UI_PREVIEW':
      return 'Job.UIPreview';
    case 'UI_INTERNAL_PREVIEW':
    case 'UI_INTERNAL_RUN':
    case 'UI_INITIAL_PREVIEW':
    case 'PREPARE_INTERNAL':
      return 'Job.Internal';
    case 'UI_EXPORT':
      return 'Job.UIDownload';
    case 'ODBC':
      if (isPrepareCreate) {
        return 'Job.ODBCCreate';
      } else if (isPrepareExecute) {
        return 'Job.ODBCExecute';
      } else if (isMetadata) {
        return 'Job.ODBCMetadataRequest';
      }
      return 'ODBCClient';
    case 'JDBC':
      if (isPrepareCreate) {
        return 'Job.JDBCCreate';
      } else if (isPrepareExecute) {
        return 'Job.JDBCExecute';
      } else if (isMetadata) {
        return 'Job.JDBCMetadataRequest';
      }
      return 'JDBCClient';
    case 'REST':
      return 'Job.RESTApp';
    case 'ACCELERATOR_CREATE':
      return 'Job.AcceleratorCreation';
    case 'ACCELERATOR_EXPLAIN':
      return 'Job.AcceleratorRefresh';
    case 'ACCELERATOR_DROP':
      return 'Job.AcceleratorRemoval';
    case 'UNKNOWN':
    default:
      return 'File.Unknown';
    }
  }

  isMetadataJob() {
    return jobsUtils.isMetadataJob(this.props.jobDetails.get('requestType'));
  }

  isDatasetAvailable() {
    const { jobDetails } = this.props;
    return !!jobDetails.get('datasetPathList');
  }

  isSingleReflectionBlockToBeShown() {
    const { jobDetails } = this.props;
    const materializationFor = jobDetails.get('materializationFor');
    return this.isDatasetAvailable() && materializationFor && materializationFor.has('reflectionType');
  }

  isDatasetBlockToBeShown() {
    const { jobDetails } = this.props;
    const materializationFor = jobDetails.get('materializationFor');
    return this.isDatasetAvailable() && materializationFor && !materializationFor.has('reflectionType');
  }

  isParentsBlockToBeShown() {
    const { jobDetails } = this.props;

    if (jobDetails.get('materializationFor')) return false; // this has no meaning to the user, so hide

    const listItem = jobDetails.getIn(['parentsList', 0]);
    return !!(listItem && (listItem.get('datasetPathList') || listItem.get('type')));
  }

  renderJobSummary(jobDetails, intl) {
    const endTime = jobsUtils.getFinishTime(jobDetails.get('state'), jobDetails.get('endTime'));
    const jobId = jobDetails.get('jobId').get('id');
    const queueName = getQueueName(jobDetails);
    const jobIdUrl = jobsUtils.navigationURLForJobId(jobId, true);
    const attemptDetails = jobDetails.get('attemptDetails');
    const haveMultipleAttempts = attemptDetails.size > 1;
    const durationLabelId = (haveMultipleAttempts) ? 'Job.TotalDuration' : 'Job.Duration';


    return (
      <div className='detail-row'>
        <h4>{intl.formatMessage({ id: 'Job.Summary' })}</h4>
        <div style={{marginBottom: 10}}>{this.renderInfoAboutProfile()}</div>
        <ul>
          <ListItem label={intl.formatMessage({ id: 'Job.QueryType' })}>
            <span>{intl.formatMessage({ id: this.getFormatMessageIdForQueryType() })}</span>
          </ListItem>
          {haveMultipleAttempts && <ListItem label={intl.formatMessage({ id: 'Job.LastAttemptDuration' })}>
            <span>{this.renderLastAttemptDuration()}</span>
          </ListItem>}
          <ListItem label={intl.formatMessage({ id: durationLabelId })}>
            <span>{this.renderJobDuration()}</span>
          </ListItem>
          <ListItem label={intl.formatMessage({ id: 'Job.StartTime' })}>
            <span>{timeUtils.formatTime(jobDetails.get('startTime'))}</span>
          </ListItem>
          <ListItem label={intl.formatMessage({ id: 'Job.EndTime' })}>
            <span>{endTime}</span>
          </ListItem>
          <ListItem label={intl.formatMessage({ id: 'Common.User' })}>
            <span>{jobDetails.get('user')}</span>
          </ListItem>
          {queueName && <ListItem label={intl.formatMessage({ id: 'Common.Queue' })}>
            <span>{queueName}</span>
          </ListItem>}
          <ListItem label={intl.formatMessage({ id: 'Job.JobID' })} style={{position: 'relative'}}>
            <span style={styles.jobId}>
              {jobId}
              <CopyButton style={{marginLeft: 5}} title={intl.formatMessage({ id: 'Common.CopyLink' })} text={jobIdUrl} />
            </span>
          </ListItem>
        </ul>
      </div>
    );
  }

  renderErrorLog() {
    const failureInfo = this.props.jobDetails.get('failureInfo');

    return (failureInfo) ? <JobErrorLog failureInfo={failureInfo} /> : null;
  }

  renderCancellationLog() {
    const cancellationInfo = this.props.jobDetails.get('cancellationInfo');

    return (cancellationInfo) ? <JobErrorLog failureInfo={cancellationInfo} /> : null;
  }

  renderDatasetBlock() {
    const { jobDetails, intl } = this.props;
    const dataset = jobDetails.getIn(['datasetPathList', -1]) || intl.formatMessage({ id: 'File.Unknown' });
    const label = intl.formatMessage({ id: 'Dataset.Dataset' });

    return (
      <ul>
        <ListItem label={label} style={{ margin: 0, alignItems: 'center' }}>
          <div style={{flex: '1 1', overflow: 'hidden', marginRight: 5}}>
            <DatasetItemLabel
              name={dataset}
              fullPath={jobDetails.get('datasetPathList')}
              showFullPath
              typeIcon={getIconByEntityType(jobDetails.get('datasetType'))}
              placement='right'
            />
          </div>
        </ListItem>
      </ul>
    );
  }

  renderSqlBlock() {
    const { jobDetails, intl } = this.props;
    if (jobDetails.get('materializationFor')) return null; // this has no meaning to the user, so hide

    const sqlString = jobDetails.get('sql');

    if (sqlString && !this.isMetadataJob()) {
      return (
        <div className='sql-wrap'>
          <h5>{`${intl.formatMessage({ id: 'SQL.SQL' })}`}</h5>
          <SqlEditor
            readOnly
            value={sqlString}
            fitHeightToContent
            maxHeight={500}
            contextMenu={false}
          />
        </div>
      );
    }
    return null;
  }

  renderSingleReflectionBlock() {
    const { jobDetails, intl } = this.props;
    const datasetPathList = jobDetails.get('datasetPathList');

    return (
      <div>
        <h4 style={{marginBottom: 10}}>{intl.formatMessage({ id: 'Job.Reflection' })}</h4>
        <ReflectionBlock datasetFullPath={datasetPathList} jobDetails={jobDetails} />
      </div>
    );
  }

  renderParentsBlock() {
    const { jobDetails } = this.props;

    return (
      <div>
        <h5><FormattedMessage id='Job.Parents'/></h5>
        <ul style={styles.parentList}>
          {jobDetails.get('parentsList').map((item, key) => {
            const dataset = item.get('datasetPathList') && item.get('datasetPathList').last();
            return (
              <li key={key} style={styles.parentItem}>
                <DatasetItemLabel
                  name={dataset}
                  fullPath={item.get('datasetPathList')}
                  typeIcon={getIconByEntityType(item.get('type'))}
                  shouldShowOverlay={item.get('type') !== undefined}
                  showFullPath
                  placement='left'/>
                <div style={{ display: 'flex' }}>
                  {false && <SettingsBtn
                    style={{ backgroundColor: 'rgba(0, 0, 0, 0.0392157)', height: 27 }}
                    hasDropdown={false}
                  />}
                  {item.get('type') &&
                    <DatasetAccelerationButton fullPath={item.get('datasetPathList')} side='left'/>
                  }
                </div>
              </li>
            );
          })}
        </ul>
      </div>
    );
  }

  renderReflectionListBlock() {
    const byRelationship = jobsUtils.getReflectionsByRelationship(this.props.jobDetails);

    return byRelationship.CHOSEN && <div>
      <h5><FormattedMessage id='Job.AcceleratedBy'/></h5>
      <ReflectionList reflections={byRelationship.CHOSEN} jobDetails={this.props.jobDetails} />
    </div>;
  }

  renderInfoAboutProfile() {
    const attemptDetails = this.props.jobDetails.get('attemptDetails');

    //should only indicate situations where a query was executed multiple times
    if (!attemptDetails || !attemptDetails.size || attemptDetails.size < 2) {
      return null;
    }

    const length = attemptDetails.size;
    //todo: loc
    const isHaveResults = OverviewContent.checkResultOfProfile(attemptDetails);
    const isHaveSchemas = OverviewContent.checkResultOfProfile(attemptDetails, 'schema');
    const isHaveMemory = OverviewContent.checkResultOfProfile(attemptDetails, 'memory');
    const initialDesc = `This query was attempted ${length} times`;
    if (!isHaveResults) {
      return initialDesc;
    } else if (!isHaveSchemas && isHaveMemory) {
      return `${initialDesc} due to insufficient memory ${isHaveMemory}`;
    } else if (isHaveSchemas && !isHaveMemory) {
      return `${initialDesc} due to schema learning ${isHaveSchemas}`;
    }
    return `${initialDesc} due to insufficient memory ${isHaveMemory} and schema learning ${isHaveSchemas}`;
  }

  renderJobDuration() {
    const { jobDetails, intl } = this.props;
    const startTime = jobDetails.get('startTime');
    const endTime = jobDetails.get('endTime');
    if (!startTime) {
      return intl.formatMessage({ id: 'Job.Pending' });
    }
    if (endTime) {
      return jobsUtils.getJobDuration(startTime, endTime);
    }
    return (
      <RealTimeTimer
        startTime={startTime}
        formatter={jobsUtils.formatJobDuration}
      />
    );
  }

  renderLastAttemptDuration() {
    const { jobDetails } = this.props;
    const attemptDetails = jobDetails.get('attemptDetails');
    const lastAttempt = attemptDetails.last();
    const totalTimeMs = lastAttempt.get('enqueuedTime') + lastAttempt.get('executionTime') + lastAttempt.get('planningTime');
    return jobsUtils.formatJobDuration(totalTimeMs);
  }


  render() {
    const { jobDetails, intl } = this.props;
    if (!jobDetails) {
      return null;
    }

    return (
      <div>
        {this.renderJobSummary(jobDetails, intl)}
        {this.renderErrorLog()}
        {this.renderCancellationLog()}

        {!this.isMetadataJob() &&
          <div className={classNames('detail-row', flexColumnContainer, flexElementAuto)}>
            {(this.isDatasetBlockToBeShown() || this.isParentsBlockToBeShown()) &&
            <h4>{intl.formatMessage({ id: 'Job.Query' })}</h4>
            }
            {this.isDatasetBlockToBeShown() && this.renderDatasetBlock()}
            {this.isSingleReflectionBlockToBeShown() && this.renderSingleReflectionBlock()}
            {this.isParentsBlockToBeShown() && this.renderParentsBlock()}
            {this.renderReflectionListBlock()}
            { jobDetails.get('stats') && <Quote jobIOData={jobDetails.get('stats')}/> }
            {this.renderSqlBlock()}
          </div>
        }
      </div>
    );
  }
}

const styles = {
  listItem: {
    margin: '5px 0',
    display: 'flex',
    alignItems: 'center'
  },
  infoValue: {
    maxWidth: 150
  },
  breadCrumbs: {
    display: 'flex',
    flexWrap: 'wrap'
  },
  icon: {
    Icon: {
      width: 29,
      height: 29
    }
  },
  parentList: {
    borderBottom: '1px solid ' + BORDER_TABLE
  },
  parentItem: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: 8,
    borderWidth: '1px 1px 0 1px',
    borderStyle: 'solid',
    borderColor: BORDER_TABLE
  },
  jobId: {
    display: 'flex',
    alignItems: 'center'
  }
};

export default OverviewContent;

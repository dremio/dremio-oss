/*
 * Copyright (C) 2017 Dremio Corporation
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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import jobsUtils from 'utils/jobsUtils';
import timeUtils from 'utils/timeUtils';
import { getIconByEntityType } from 'utils/iconUtils';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import DatasetAccelerationButton from 'dyn-load/components/Acceleration/DatasetAccelerationButton';
import SettingsBtn from 'components/Buttons/SettingsBtn';
import LinkButton from 'components/Buttons/LinkButton';
import RealTimeTimer from 'components/RealTimeTimer';
import CopyButton from 'components/Buttons/CopyButton';
import CodeMirror from 'components/CodeMirror';
import { injectIntl, FormattedMessage } from 'react-intl';

import { BORDER_TABLE } from 'uiTheme/radium/colors';

import Quote from './Quote';
import ListItem from './ListItem';
import JobErrorLog from './JobErrorLog';
import ReflectionList from './ReflectionList';

import 'codemirror/mode/sql/sql';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/mdn-like.css';

@injectIntl
@Radium
@PureRender
class OverviewContent extends Component {
  static checkResultOfProfile = (attemptDetails, reason = '') => {
    if (!reason) {
      return attemptDetails.find( profile => profile.get('reason') );
    }
    return attemptDetails.filter( profile => profile.get('reason').indexOf(reason) !== -1 ).size;
  };

  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    failureInfo: PropTypes.string,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static codeMirrorOptions = {
    readOnly: true,
    lineWrapping: true
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

  isParentsAvailable() {
    const { jobDetails } = this.props;

    if (jobDetails.get('materializationFor')) return false; // this has no meaning to the user, so hide

    const listItem = jobDetails.getIn(['parentsList', 0]);
    return !!(listItem && (listItem.get('datasetPathList') || listItem.get('type')));
  }

  renderErrorLog() {
    const { failureInfo } = this.props;
    return failureInfo ? (
      <JobErrorLog error={failureInfo} />
    ) : null;
  }

  renderSqlBlock() {
    const { jobDetails, intl } = this.props;
    if (jobDetails.get('materializationFor')) return null; // this has no meaning to the user, so hide

    const sqlString = jobDetails.get('sql');
    if (sqlString && !this.isMetadataJob()) {
      return (
        <div className='sql-wrap'>
          <h5>{`${intl.formatMessage({ id: 'SQL.SQL' })}`}</h5>
          <CodeMirror
            defaultValue={sqlString}
            options={OverviewContent.codeMirrorOptions} />
        </div>
      );
    }
    return null;
  }

  renderParentsBlock() {
    const { jobDetails } = this.props;
    if (!this.isParentsAvailable()) {
      return null;
    }

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

  renderReflectionsBlock() {
    const byRelationship = jobsUtils.getReflectionsByRelationship(this.props.jobDetails);

    return byRelationship.CHOSEN && <div>
      <h5><FormattedMessage id='Job.AcceleratedBy'/></h5>
      <ReflectionList reflections={byRelationship.CHOSEN} jobDetails={this.props.jobDetails} />
    </div>;
  }

  renderDatasetBlock() {
    const { jobDetails, intl } = this.props;
    if (!this.isDatasetAvailable()) return;
    const dataset = jobDetails.getIn(['datasetPathList', -1]) || intl.formatMessage({ id: 'File.Unknown' });
    const materializationFor = jobDetails.get('materializationFor');
    const label = intl.formatMessage({ id: materializationFor ? 'Job.ReflectionForDataset' : 'Dataset.Dataset' });

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
          { materializationFor && <LinkButton to={{
            ...this.context.location,
            state: {
              modal: 'AccelerationModal',
              accelerationId: materializationFor.get('accelerationId'),
              layoutId: materializationFor.get('layoutId')
            }
          }}>{intl.formatMessage({ id: 'Job.ShowReflection' })}</LinkButton>}

        </ListItem>
      </ul>
    );
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

  render() {
    const { jobDetails, intl } = this.props;
    if (!jobDetails) {
      return null;
    }
    const endTime = jobsUtils.getFinishTime(jobDetails.get('state'), jobDetails.get('endTime'));
    const quoteStyle = this.isDatasetAvailable() || this.isParentsAvailable()
      ? {display: 'block'}
      : {display: 'none'};

    const jobId = jobDetails.get('jobId').get('id');
    const jobIdUrl = jobsUtils.navigationURLForJobId(jobId, true);

    return (
      <div>
        <div className='detail-row'>
          <h4>{intl.formatMessage({ id: 'Job.Summary' })}</h4>
          <div style={{marginBottom: 10}}>{this.renderInfoAboutProfile()}</div>
          <ul>
            <ListItem label={intl.formatMessage({ id: 'Job.QueryType' })}>
              <span>{intl.formatMessage({ id: this.getFormatMessageIdForQueryType() })}</span>
            </ListItem>
            <ListItem label={intl.formatMessage({ id: 'Job.Duration' })}>
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
            <ListItem label={intl.formatMessage({ id: 'Job.JobID' })} style={{position: 'relative'}}>
              <span style={styles.jobId}>
                {jobId}
                <CopyButton style={{marginLeft: 5}} title={intl.formatMessage({ id: 'Common.CopyLink' })} text={jobIdUrl} />
              </span>
            </ListItem>
          </ul>
        </div>
        {this.renderErrorLog()}

        {!this.isMetadataJob() &&
          <div className='detail-row'>
            <h4 style={[quoteStyle]}>{intl.formatMessage({ id: 'Job.Query' })}</h4>
            {this.renderDatasetBlock()}
            {this.renderParentsBlock()}
            {this.renderReflectionsBlock()}
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

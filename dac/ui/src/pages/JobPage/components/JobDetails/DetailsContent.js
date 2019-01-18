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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import jobsUtils from 'utils/jobsUtils';
import FileUtils from 'utils/FileUtils';
import NumberFormatUtils from 'utils/numberFormatUtils';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import FontIcon from 'components/Icon/FontIcon';
import CodeMirror from 'components/CodeMirror';

import { BORDER_TABLE } from 'uiTheme/radium/colors';

import VisibilityToggler from './VisibilityToggler';
import ListItem from './ListItem';

@Radium
@PureRender
class DetailsContent extends Component {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map)
  };

  static codeMirrorOptions = {
    readOnly: true
  };

  static getProcessType(type) {
    const hash = {
      MISC: 'Misc',
      SORT: 'Sort',
      JOIN: 'Join',
      DATA_EXCHANGE: 'Data Exchange',
      READING: 'Reading',
      CLIENT: 'Client',
      FILTER: 'Filter',
      PROJECT: 'Project',
      LIMIT: 'Limit'
    };
    return hash[type] || type;
  }

  static getCpuUsage(value) {
    if (value >= 10) {
      return Number(value) && Number(value).toFixed(0);
    } else if (value >= 1) {
      return Number(value) && Number(value).toFixed(1);
    }
    return '< 0.1';
  }

  constructor(props) {
    super(props);
  }

  getCodeMirrorItem(code) {
    return (
      <ListItem label={la('Push-Down Query')}>
        <CodeMirror
          defaultValue={code}
          options={DetailsContent.codeMirrorOptions} />
      </ListItem>
    );
  }

  getFsDatasetData(item) {
    return (
      <ul>
        {item.get('prunedPathsList') ? (
          <ListItem label={la('Paths Pruned')}>
            <span>{item.get('prunedPathsList')}</span>
          </ListItem>
        ) : null}
        <ListItem label={la('Data Volume')}>
          <span>{item.get('dataVolumeInBytes')}</span>
        </ListItem>
      </ul>
    );
  }

  getDatasetPathList(datasets) {
    return datasets && datasets.map( (dataset, key) => {
      return (
        <DatasetItemLabel
          key={key}
          name={dataset.get('datasetPathList').last()}
          fullPath={dataset.get('datasetPathList')}
          placement='right'
          showFullPath
          typeIcon='VirtualDataset' // TODO need server to return dataset type DX-5885
        />
      );
    });
  }

  getJointDatasetList(tableDatasetList) {
    return tableDatasetList && tableDatasetList.map && tableDatasetList.map((item, index) => {
      const datasetProfile = item.get('datasetProfile') || Immutable.Map();
      const title = <span style={[styles.flexAlign, { paddingRight: 7, flexGrow: 1 }]}>
        {`(${datasetProfile.get('datasetPathsList').size}) `}
        {this.getDatasetPathList(datasetProfile.get('datasetPathsList'))}
        {/* Note: item.get('accelerated') not received pending DX-5519 */}
        {item.get('accelerated') && <FontIcon type='Flame' style={{marginLeft: 'auto'}} />}
      </span>;

      return (
        <VisibilityToggler
          style={styles.jointDataset}
          key={index}
          title={title}
        >
          <div style={{ borderTop: `1px solid ${BORDER_TABLE}`, padding: '8px 10px' }}>
            <ul>
              <ListItem label={la('Parallelism')}>
                <span>{datasetProfile.get('parallelism')}</span>
              </ListItem>
              <ListItem label={la('Average Wait on Source')}>
                <span>{jobsUtils.msToHHMMSS(datasetProfile.get('waitOnSource'))}</span>
              </ListItem>
              <ListItem label={la('Read')}>
                <span>{FileUtils.getFormattedBytes(datasetProfile.get('bytesRead'))}</span>
              </ListItem>
              <ListItem label={la('Records Read')}>
                <span>{datasetProfile.get('recordsRead')}</span>
              </ListItem>
              {datasetProfile.get('pushdownQuery') ? this.getCodeMirrorItem(datasetProfile.get('pushdownQuery')) : null}
              {datasetProfile.get('dataVolumeInBytes') ? this.getFsDatasetData(item) : null}
            </ul>
          </div>
        </VisibilityToggler>
      );
    });
  }

  getTopOperations(topOperations) {
    if (!topOperations) {
      return null;
    }
    return (
      <ul style={styles.topOperations}>
        {topOperations.map && topOperations.map((item, index) => {
          const cpuUsage = DetailsContent.getCpuUsage(item.get('timeConsumed'));
          return (
            <ListItem key={index}>
              <div style={styles.operationName}>{DetailsContent.getProcessType(item.get('type'))}</div>
              <div style={styles.dataLine}><span style={[styles.lineGraph, {width: item.get('timeConsumed')}]} /></div>
              <div style={styles.cpu}>{cpuUsage}% CPU</div>
            </ListItem>
          );
        })}
      </ul>
    );
  }

  getSpilledOperations(spillDetails) {
    if (!spillDetails) return null;

    const haveAgg = spillDetails.get('hashAggSpilled');
    const haveSort = spillDetails.get('sortSpilled');
    const operations = haveAgg && haveSort && la('Aggregate') || haveAgg && la('Aggregate') || haveSort && la('Sort');
    return (
      <ListItem label={la('Spilled Operations:')}>
        <span>{operations}</span>
      </ListItem>
    );
  }

  getTotalSpilled(spillDetails) {
    if (!spillDetails) return null;

    const memoryValue = spillDetails.get('totalBytesSpilled', 0);
    const memoryString = NumberFormatUtils.makeMemoryValueString(memoryValue);
    return (
      <ListItem label={la('Total Spilled:')}>
        <span>{memoryString}</span>
      </ListItem>
    );
  }

  render() {
    const { jobDetails } = this.props;
    const datasetsList = jobDetails.get('tableDatasetProfiles');
    const spilledDetails = jobDetails.get('spilled') && jobDetails.get('spillDetails');
    const attemptDetails = jobDetails.get('attemptDetails');
    const lastAttempt = attemptDetails.last();

    return (
      <div>
        <div className='detail-row'>
          <h4>{la('Plan')}</h4>
          <ul>
            <ListItem label={la('Planning Time')}>
              <span>{jobsUtils.msToHHMMSS(lastAttempt && lastAttempt.get('planningTime'))}</span>
            </ListItem>
            <ListItem label={la('Enqueued Time')}>
              <span>{jobsUtils.formatJobDuration(lastAttempt && lastAttempt.get('enqueuedTime'))}</span>
            </ListItem>
          </ul>
        </div>
        <div className='detail-row'>
          <h4>{la('Read')}</h4>
          {this.getJointDatasetList(datasetsList)}
        </div>
        <div className='detail-row'>
          <h4>{la('Process')}</h4>
          <ul>
            <ListItem label={la('Execution Time')}>
              <span>{jobsUtils.formatJobDuration(lastAttempt && lastAttempt.get('executionTime'))}</span>
            </ListItem>
            <ListItem label={la('Top Operations')}>
              {this.getTopOperations(jobDetails.get('topOperations'))}
            </ListItem>
            {this.getSpilledOperations(spilledDetails)}
            {this.getTotalSpilled(spilledDetails)}
          </ul>
        </div>
        <div className='detail-row'>
          <h4>{la('Return')}</h4>
          <ul>
            <ListItem label={la('Wait on Client')}>
              <span>{jobsUtils.msToHHMMSS(jobDetails.get('waitInClient'))}</span>
            </ListItem>
            <ListItem label={la('Number of Records')}>
              <span>{jobDetails.get('outputRecords')}</span>
            </ListItem>
            <ListItem label={la('Data Volume')}>
              <span>{jobDetails.get('dataVolume')}</span>
            </ListItem>
          </ul>
        </div>
      </div>
    );
  }
}

const styles = {
  jointDataset: {
    border: `1px solid ${BORDER_TABLE}`,
    marginBottom: 7
  },
  verticalIndent: {
    margin: '5px 0'
  },
  flexAlign: {
    display: 'flex',
    alignItems: 'center'
  },
  topOperations: {
    clear: 'both',
    overflow: 'hidden'
  },
  operationName: {
    minWidth: 90,
    padding: '0 5px 0 0'
  },
  dataLine: {
    width: 150,
    margin: '0 10px 0 0'
  },
  lineGraph: {
    height: 8,
    background: '#77818f',
    display: 'inline-block'
  },
  cpu: {
    color: '#999'
  }
};

export default DetailsContent;

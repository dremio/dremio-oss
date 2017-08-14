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
import { Component, PropTypes } from 'react';
import { connect } from 'react-redux';
import Radium from 'radium';
import Immutable from 'immutable';
import PureRender from 'pure-render-decorator';
import { Link } from 'react-router';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import SimpleButton from 'components/Buttons/SimpleButton';
import FontIcon from 'components/Icon/FontIcon';
import StateIconTypes from 'constants/jobPage/StateIconType.json';
import StateLabels from 'constants/jobPage/StateLabels.json';
import datasetPathUtils from 'utils/resourcePathUtils/dataset';
import { constructFullPathAndEncode, constructResourcePath } from 'utils/pathUtils';
import { getViewState } from 'selectors/resources';

import { h4 } from 'uiTheme/radium/typography';

@PureRender
@Radium
class HeaderDetails extends Component {
  static propTypes = {
    jobState: PropTypes.string.isRequired,
    cancelJob: PropTypes.func,
    style: PropTypes.object,
    jobId: PropTypes.string.isRequired,
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadFile: PropTypes.func
  };

  static contextTypes = {
    location: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.cancelJob = this.cancelJob.bind(this);
    this.openJobResults = this.openJobResults.bind(this);
  }

  getButton(jobState) {
    const { jobDetails, jobId } = this.props;

    // if the query is running or pending, expose the cancel button.
    if (jobState === 'RUNNING' || jobState === 'PENDING') {
      return (
        <Button
          type={ButtonTypes.CUSTOM}
          text={la('Cancel')}
          onClick={this.cancelJob}
          styles={[styles.button]}/>);
    }

    const queryType = jobDetails.get('queryType');
    // don't show the open results link if there are no results available or if not a completed UI query.
    // TODO: show a message if the results are not available (DX-7459)
    if (!jobDetails.get('resultsAvailable') || jobState !== 'COMPLETED'
        || (queryType !== 'UI_PREVIEW' && queryType !== 'UI_RUN' && queryType !== 'UI_EXPORT')) {
      return null;
    }

    // if this is a export dataset and we have a download url, show the download button.
    if (queryType === 'UI_EXPORT') {
      const isCanDownloaded = jobDetails.get('downloadUrl');
      return (
        <SimpleButton
          onClick={this.downloadDatasetFile}
          buttonStyle='primary'
          disabled={!isCanDownloaded}
          children={la('Download')}
          submitting={this.props.downloadViewState.get('isInProgress')}
          style={[styles.button, {marginLeft: 10}]}
          type='button'/>);
    }

    // determine the full path of the item. If we have a root datasetPathList, use that. If not, use the first parent's
    // datasetPathList.
    const datasetFullPath = jobDetails.get('datasetPathList')
        || jobDetails.getIn(['parentsList', 0, 'datasetPathList']);

    const fullPath = constructFullPathAndEncode(datasetFullPath);
    const resourcePath  = constructResourcePath(fullPath);

    const nextLocation = {
      pathname: datasetPathUtils.toHref(resourcePath),
      query: {jobId, version: jobDetails.get('datasetVersion')}
    };

    // make sure we get back into the right mode for "edit" queries
    if (jobDetails.get('datasetPathList')) {
      nextLocation.query.mode = 'edit';
    }

    return (
      <span style={styles.openResults}>
        <FontIcon type='VirtualDataset' />
        <Link data-qa='open-results-link' to={nextLocation}>
          {la('Open Results')} »
        </Link>
      </span>
    );
  }

  downloadDatasetFile = () => {
    const viewId = this.props.downloadViewState.get('viewId');
    this.props.downloadFile({url: this.props.jobDetails.get('downloadUrl'), viewId});
  };

  openJobResults() {}

  cancelJob() {
    this.props.cancelJob();
  }

  render() {
    const { jobState, style, jobDetails } = this.props;
    const label = StateLabels[jobState];

    if (!jobState) {
      return null;
    }

    return (
      <header className='details-header' style={[styles.detailsHeader, style]}>
        <div style={styles.stateHolder}>
          <FontIcon theme={styles.stateIcon} type={StateIconTypes[jobState]}/>
          <div className='state'>
            {label} <span style={[styles.state, h4]}>{jobState.toLowerCase()}</span>
          </div>
          {jobDetails.get('accelerated') && <FontIcon type='Flame' style={{ marginLeft: 5 }} />}
        </div>
        <div style={[styles.rightPart]}>
          {this.getButton(jobState)}
        </div>
      </header>
    );
  }
}
const styles = {
  button: {
    'display': 'inline-flex',
    'float': 'right',
    'margin': '0 0 0 10px',
    'width': 120,
    'height': 28
  },
  state: {
    textTransform: 'capitalize'
  },
  openResults: {
    display: 'flex',
    alignItems: 'center',
    margin: '0 0 0 5px'
  },
  stateHolder: {
    display: 'flex',
    alignItems: 'center'
  },
  rightPart: {
    display: 'flex',
    justifyContent: 'flex-end',
    width: '100%'
  },
  detailsHeader: {
    height: 38,
    background: 'rgba(0,0,0,.05)',
    display: 'flex',
    alignItems: 'center',
    padding: '0 10px 0 5px'
  },
  stateIcon: {
    Container: {
      margin: '3px 0 0'
    }
  }
};

function mapStateToProps(state, props) {
  return {
    downloadViewState: getViewState(state, `DOWNLOAD_JOB_RESULTS-${props.jobId}`)
  };
}

export default connect(mapStateToProps)(HeaderDetails);

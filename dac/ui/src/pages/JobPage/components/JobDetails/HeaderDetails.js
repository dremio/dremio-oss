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
import { connect } from 'react-redux';
import Radium from 'radium';
import Immutable from 'immutable';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import { injectIntl } from 'react-intl';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import SimpleButton from 'components/Buttons/SimpleButton';
import Art from 'components/Art';
import datasetPathUtils from 'utils/resourcePathUtils/dataset';
import { constructFullPathAndEncode, constructResourcePath } from 'utils/pathUtils';
import { getViewState } from 'selectors/resources';
import JobsUtils, { JobState } from '@app/utils/jobsUtils';

import JobStateIcon from '../JobStateIcon';

@injectIntl
@PureRender
@Radium
class HeaderDetails extends Component {
  static propTypes = {
    cancelJob: PropTypes.func,
    style: PropTypes.object,
    jobId: PropTypes.string.isRequired,
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadFile: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.cancelJob = this.cancelJob.bind(this);
    this.openJobResults = this.openJobResults.bind(this);
  }

  getButton() {
    const { jobDetails, jobId, intl } = this.props;
    const currentJobState = jobDetails.get('state');

    // if the query is running or pending, expose the cancel button.
    if (JobsUtils.getRunning(currentJobState) || currentJobState === JobState.ENQUEUED) {
      return (
        <Button
          type={ButtonTypes.CUSTOM}
          text={intl.formatMessage({id: 'Common.Cancel'})}
          onClick={this.cancelJob}
          styles={[styles.button]}/>);
    }

    const queryType = jobDetails.get('queryType');
    // don't show the open results link if there are no results available or if not a completed UI query.
    // TODO: show a message if the results are not available (DX-7459)
    if (!jobDetails.get('resultsAvailable') || currentJobState !== JobState.COMPLETED
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
          children={intl.formatMessage({id: 'Download.Download'})}
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
        <Art
          src={'VirtualDataset.svg'}
          alt={intl.formatMessage({id: 'Dataset.VirtualDataset'})}
          style={styles.virtualDatasetIcon} />
        <Link data-qa='open-results-link' to={nextLocation}>
          {intl.formatMessage({id: 'Job.OpenResults'})} »
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
    const { style, jobDetails, intl } = this.props;

    if (!jobDetails.get('state')) {
      return null;
    }

    const flame = jobDetails.get('snowflakeAccelerated') ? 'FlameSnowflake.svg' : 'Flame.svg';
    const flameAlt = jobDetails.get('snowflakeAccelerated') ? 'Job.AcceleratedHoverSnowFlake' : 'Job.AcceleratedHover';

    return (
      <header className='details-header' style={[styles.detailsHeader, style]}>
        <div style={styles.stateHolder}>
          <JobStateIcon state={jobDetails.get('state')} />
          <div className='state'>
            <span className='h4' style={[styles.state]}>
              {intl.formatMessage({id: 'Job.State.' + jobDetails.get('state')})}
            </span>
          </div>
          {jobDetails.get('accelerated') &&
            <Art src={flame} alt={intl.formatMessage({id: flameAlt})} style={styles.flameIcon} title/>}
          {jobDetails.get('spilled') &&
            <Art src='DiskSpill.svg' alt={intl.formatMessage({id: 'Job.SpilledHover'})} style={styles.flameIcon} title/>}
        </div>
        <div style={[styles.rightPart]}>
          {this.getButton()}
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
    marginBottom: '0'
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
    width: 24,
    height: 24
  },
  virtualDatasetIcon: {
    width: 24,
    height: 24,
    marginTop: -4
  },
  flameIcon: {
    width: 20,
    height: 20,
    marginLeft: 5,
    marginTop: -2
  }
};

function mapStateToProps(state, props) {
  return {
    downloadViewState: getViewState(state, `DOWNLOAD_JOB_RESULTS-${props.jobId}`)
  };
}

export default connect(mapStateToProps)(HeaderDetails);

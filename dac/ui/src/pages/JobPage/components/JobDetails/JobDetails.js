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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import uuid from 'uuid';

import ViewStateWrapper from 'components/ViewStateWrapper';

import { loadJobDetails, cancelJobAndShowNotification, showJobProfile } from 'actions/jobs/jobs';
import { downloadFile } from 'sagas/downloadFile';
import socket from 'utils/socket';
import { getViewState } from 'selectors/resources';
import { getEntity } from 'selectors/resources';
import { updateViewState } from 'actions/resources';
import { flexElementAuto } from '@app/uiTheme/less/layout.less';

import HeaderDetails from './HeaderDetails';
import TabsNavigation from './TabsNavigation';
import TabsContent from './TabsContent';
import './JobDetails.less';

const VIEW_ID = 'JOB_DETAILS_VIEW_ID';

@Radium
@PureRender
export class JobDetails extends Component {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    jobId: PropTypes.string,
    location: PropTypes.object,
    askGnarly: PropTypes.func,

    // actions
    loadJobDetails: PropTypes.func,
    cancelJob: PropTypes.func,
    downloadFile: PropTypes.func,
    showJobProfile: PropTypes.func,
    updateViewState: PropTypes.func,

    //connected
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  refreshInterval = 0;

  defaultProps = {
    jobDetails: Immutable.Map()
  }

  constructor(props) {
    super(props);
    this.receiveProps(props, {});
  }

  state = {
    activeTab: 'overview'
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  componentWillUnmount() {
    this.stopListenToJobChange(this.props.jobId);
    clearInterval(this.refreshInterval);
  }

  receiveProps(nextProps, oldProps) {
    const jobId = nextProps.jobId;
    const oldJobId = oldProps.jobId;

    if (jobId && jobId !== oldJobId) {
      this.stopListenToJobChange(oldJobId);
      this.load(jobId);
      clearInterval(this.refreshInterval);
      this.refreshInterval = setInterval(this.load, 3000);
    }
  }

  load = (jobId = this.props.jobId) => {
    return this.props.loadJobDetails(jobId, VIEW_ID).then((response) => {
      if (!response || (response.error && !response.payload)) return; // no-payload error check for DX-9340

      if (response.meta.jobId !== jobId) return;

      if (!response.error) {
        socket.startListenToJobChange(jobId);
      } else if (response.payload.status === 404) {
        this.props.updateViewState(VIEW_ID, {
          isFailed: false,
          isWarning: true,
          error: {
            message: la('Could not find the specified job\'s details, they may have been cleaned up.'),
            id: uuid.v4()
          }
        });
      }
    });
  }

  stopListenToJobChange(jobId) {
    if (jobId) {
      socket.stopListenToJobChange(jobId);
    }
  }

  cancelJob = () => {
    this.props.cancelJob(this.props.jobId);
  }

  changeTab = (id) => this.setState({ activeTab: id})

  render() {
    const { jobId, jobDetails, viewState } = this.props;
    const haveDetails = !!(jobId && jobDetails && !jobDetails.isEmpty());

    return (
      <section className='job-details' style={styles.base}>
        <ViewStateWrapper viewState={viewState} hideChildrenWhenInProgress={false} hideChildrenWhenFailed={false} hideSpinner={haveDetails}>
          { haveDetails &&
            <div style={{height: '100%', display: 'flex', flexDirection: 'column'}}>
              <HeaderDetails
                cancelJob={this.cancelJob}
                jobId={jobId}
                style={styles.header}
                jobDetails={jobDetails}
                downloadFile={this.props.downloadFile}
              />
              <TabsNavigation
                changeTab={this.changeTab}
                activeTab={this.state.activeTab}
                style={styles.header}
                attemptDetails={jobDetails.get('attemptDetails')}
                showJobProfile={this.props.showJobProfile}
                location={this.props.location}
              />
              <TabsContent
                jobId={jobId}
                jobDetails={jobDetails}
                showJobProfile={this.props.showJobProfile}
                changeTab={this.changeTab}
                activeTab={this.state.activeTab}
                className={flexElementAuto} // take a rest of available space
              />
            </div>
          }
        </ViewStateWrapper>
      </section>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    display: 'flex',
    flexDirection: 'column',
    margin: '0 0 0 10px',
    overflowY: 'auto',
    flex: '1 1',
    background: '#fff'
  },
  header: {
    flexShrink: 0
  }
};

function mapStateToProps(state, ownProps) {
  return {
    viewState: getViewState(state, VIEW_ID),
    jobDetails: getEntity(state, ownProps.jobId, 'jobDetails')
  };
}


export default connect(mapStateToProps, {
  cancelJob: cancelJobAndShowNotification,
  loadJobDetails,
  downloadFile,
  showJobProfile,
  updateViewState
})(JobDetails);

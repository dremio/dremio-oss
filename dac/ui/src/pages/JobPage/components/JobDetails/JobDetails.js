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
import PureRender from 'pure-render-decorator';
import Immutable from 'immutable';
import uuid from 'uuid';

import ViewStateWrapper from 'components/ViewStateWrapper';

import { body } from 'uiTheme/radium/typography';
import { loadJobDetails, cancelJobAndShowNotification, showJobProfile } from 'actions/jobs/jobs';
import { downloadFile } from 'sagas/downloadFile';
import socket from 'utils/socket';
import { getViewState } from 'selectors/resources';
import { getEntity } from 'selectors/resources';
import { updateViewState } from 'actions/resources';

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
  }

  receiveProps(nextProps, oldProps) {
    const jobId = nextProps.jobId;
    const oldJobId = oldProps.jobId;

    if (jobId && jobId !== oldJobId) {
      this.stopListenToJobChange(oldJobId);
      this.props.loadJobDetails(jobId, VIEW_ID).then((response) => {
        if (!response) return;

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

    return (
      <section className='job-details' style={styles.base}>
        <ViewStateWrapper viewState={viewState}>
          { jobId && jobDetails && !jobDetails.isEmpty() &&
            <div style={{height: '100%', display: 'flex', flexDirection: 'column'}}>
              <HeaderDetails
                jobState={jobDetails.get('state')}
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
                style={styles.content}
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
    ...body,
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
  },
  content: {
    flex: 1,
    overflowY: 'auto'
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

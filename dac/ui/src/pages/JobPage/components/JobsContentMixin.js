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

import jobsUtils from '@app/utils/jobsUtils.js';
import jobUtils from 'utils/jobsUtils';

// export this for calculate min width of table tr in JobTable.js
export const SEPARATOR_WIDTH = 10;
export const MIN_LEFT_PANEL_WIDTH = 500;
export const MIN_RIGHT_PANEL_WIDTH = 330;

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getActiveJob() {
      const { jobId } = this.props;
      if (jobId) {
        return this.findCurrentJob(this.props, jobId);
      }
    },

    findCurrentJob(props, jobId) {
      return props.jobs.size && props.jobs.find((item) => item.get('id') === jobId);
    },

    runActionForJobs(jobs, isStop, callback) {
      jobs.forEach((job) => {
        const jobId = job.get('id');
        const jobState = job.get('state');
        if (isStop || jobUtils.isJobRunning(jobState)) {
          return callback(jobId);
        }
      });
    },

    handleMouseReleaseOutOfBrowser() {
      if (this.state.isResizing) {
        this.handleEndResize();
      }
    },

    handleStartResize() {
      this.setState({ isResizing: true });
    },

    handleEndResize() {
      this.setState({
        isResizing: false,
        width: typeof this.state.left === 'number' ? this.state.left - SEPARATOR_WIDTH : this.state.width });
    },

    handleResizeJobs(e) {
      const left = e && (e.clientX - SEPARATOR_WIDTH / 2);
      if (this.state.isResizing && left > MIN_LEFT_PANEL_WIDTH) {
        const width = document.body.offsetWidth - left;
        if (width > MIN_RIGHT_PANEL_WIDTH) {
          this.setState({
            left
          });
        }
      }
    },

    setActiveJob(jobData, isReplaceUrl) {
      const { location } = this.props;
      const router = this.context.router[isReplaceUrl ? 'replace' : 'push'];
      if (jobData && !jobsUtils.isNewJobsPage()) {
        router({...location, pathname: '/jobs', hash: `#${jobData.get('id')}`});
      } else {
        router({...location, hash: ''});
      }
    },

    styles: {
      base: {
        position: 'relative',
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        width: '100%'
      },
      viewState: {
        display: 'flex',
        // this is needed to force a view state wrapper fit to parent and do not overflow it
        // as this cause a scrollbar to appear
        minHeight: 0
      },
      filters: {
        flexShrink: 0
      },
      jobWrapper: {
        flex: 1,
        position: 'relative',
        overflow: 'hidden',
        display: 'flex',
        backgroundColor: '#ccc',
        padding: '10px'
      },
      separator: {
        width: 10,
        background: '#ccc',
        cursor: 'col-resize',
        position: 'absolute',
        top: 0,
        bottom: 0,
        zIndex: 999,
        left: '50%',
        opacity: '.6'
      },
      noSelection: {
        userSelect: 'none'
      }
    }
  });
}

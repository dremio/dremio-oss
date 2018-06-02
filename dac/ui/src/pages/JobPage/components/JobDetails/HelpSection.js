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
import Immutable from 'immutable';
import { connect } from 'react-redux';

import FontIcon from 'components/Icon/FontIcon';
import './HelpSection.less';

import { askGnarly } from 'actions/jobs/jobs';
import { callIfChatAllowedOrWarn } from 'actions/account';
import { downloadFile } from 'sagas/downloadFile';

import { getViewState } from 'selectors/resources';

import jobsUtils from 'utils/jobsUtils';
import config from 'utils/config';

import HelpSectionMixin from 'dyn-load/pages/JobPage/components/JobDetails/HelpSectionMixin';


// todo: loc
const MESSAGES = {
  chat: `Need help troubleshooting query performance or understanding the results?
Click “Ask Dremio” to share the query profile with a Dremio engineer who will help you out.`,
  email: `Need help troubleshooting query performance or understanding the results?
Click “Email Help” to share job information with your system administrator.`,
  chatAndEmail: `Need help troubleshooting query performance or understanding the results?
Click “Ask Dremio” to share the query profile with Dremio Support or “Email Help” to share it with your system administrator.`,
  download: 'Click “Download Profile” to download the query profile.'
}; // download is never called out in the message unless it's the only thing

@HelpSectionMixin
export class HelpSection extends PureComponent {
  static propTypes = {
    jobId: PropTypes.string.isRequired,

    // connected:
    callIfChatAllowedOrWarn: PropTypes.func.isRequired,
    askGnarly: PropTypes.func.isRequired,
    downloadFile: PropTypes.func.isRequired,
    downloadViewState: PropTypes.instanceOf(Immutable.Map).isRequired
  };

  static contextTypes = {
    loggedInUser: PropTypes.object
  };

  handleDownload = () => {
    this.props.downloadFile({
      url: `/support/${this.props.jobId}/download`,
      method: 'POST',
      viewId: this.props.downloadViewState.get('viewId')
    });
  }

  handleEmail = () => {
    const subject = config.supportEmailSubjectForJobs || la('Can I get some help on a Dremio job?');
    const body = la('Hi,\n\nCan you help me with this Dremio job?\n\n')
      + window.location.origin + jobsUtils.navigationURLForJobId(this.props.jobId)
      + '\n\n';
    const mailto = 'mailto:' + encodeURIComponent(config.supportEmailTo)
      + '?subject=' + encodeURIComponent(subject)
      + '&body=' + encodeURIComponent(body);

    // We don't want the mailto to override the Dremio tab if the user uses webmail.
    // But we also don't want it to leave a blank tab if the user uses an app.
    // So we give the browser 5s to figure out how to handle it.
    // That's probably way too long most of the time, but it's gated on latency to your webmail. :(
    // Luckily, if you use webmail everything looks fine, and if you have native mail
    // you just switched to a completely different app.
    // Tested in Chrome/Mac, Firefox/Mac with Apple Mail and Gmail webmail.
    const newWindow = window.open(mailto, '_blank');
    setTimeout(() => {
      try {
        const href = newWindow.location.href; // will `throw` if webmail
        if (href === 'about:blank') {
          newWindow.close();
        }
      } catch (e) {
        // loaded webmail and now security blocked
      }
    }, 5000);
  }

  render()  {
    const buttons = this.getButtons();
    if (!buttons.size) return null;

    let message = MESSAGES.download;
    if (buttons.has('chat') && buttons.has('email')) {
      message = MESSAGES.chatAndEmail;
    } else if (buttons.has('chat')) {
      message = MESSAGES.chat;
    } else if (buttons.has('email')) {
      message = MESSAGES.email;
    }

    return <div className='help-section'>
      <h4>{la('Help')}</h4>
      <div className='quote-wrapper'>
        <div className={'dremioLogo'}>
          <FontIcon type='NarwhalLogo' theme={styles.NarwhalLogo} />
        </div>
        <div className='quote-block'>
          <div>{message}</div>
          <div children={buttons.toArray()}/>
        </div>
      </div>
    </div>;
  }
}

function mapStateToProps(state, props) {
  return {
    downloadViewState: getViewState(state, `DOWNLOAD_JOB_PROFILE-${props.jobId}`)
  };
}

export default connect(mapStateToProps, {
  askGnarly,
  callIfChatAllowedOrWarn,
  downloadFile
})(HelpSection);


const styles = {
  NarwhalLogo: {
    Container: {
      width: 100,
      height: 100,
      float: 'left',
      margin: '0 10px 0 0'
    },
    Icon: {
      width: 100,
      height: 100
    }
  }
};

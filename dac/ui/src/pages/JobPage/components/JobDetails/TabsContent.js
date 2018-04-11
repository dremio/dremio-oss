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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import Tabs from 'components/Tabs';
import OverviewContent from './OverviewContent';
import DetailsContent from './DetailsContent';
import AccelerationContent from './AccelerationContent';
import ProfilesContent from './ProfilesContent';
import HelpSection from './HelpSection';

@Radium
class TabsContent extends Component {
  static propTypes = {
    jobId:  PropTypes.string,
    activeTab: PropTypes.string,
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    showJobProfile: PropTypes.func,
    style: PropTypes.object
  };

  componentWillReceiveProps(nextProps) {
    if (nextProps.activeTab !== this.props.activeTab) {
      this.refs.holder.scrollTop = 0;
    }
  }

  renderTabsContent = () => {
    const { jobId, activeTab, jobDetails } = this.props;

    if (jobDetails && jobDetails.get('isEmptyJob')) {
      return (
        <div style={[styles.spinner]}>
          <span style={{fontSize: 20}}>{la('No job info available.')}</span>
        </div>
      );
    }

    return (
      <Tabs activeTab={activeTab}>
        <OverviewContent
          tabId='overview'
          jobId={jobId}
          jobDetails={jobDetails}
        />
        <DetailsContent
          tabId='details'
          jobId={jobId}
          jobDetails={jobDetails} />
        <AccelerationContent
          tabId='acceleration'
          jobId={jobId}
          jobDetails={jobDetails} />
        <ProfilesContent
          tabId='profiles'
          jobId={jobId}
          showJobProfile={this.props.showJobProfile}
          jobDetails={jobDetails} />
      </Tabs>
    );
  }

  render() {
    const {style} = this.props;
    const content = this.renderTabsContent();

    return (
      <div ref='holder' className='content-holder' style={[styles.base, style]}>
        <div style={{flex: '1 0 auto'}}>
          {content}
        </div>
        <HelpSection jobId={this.props.jobId} />
      </div>
    );
  }
}

export default TabsContent;

const styles = {
  base: {
    position: 'relative',
    display: 'flex',
    flexDirection: 'column'
  },
  spinner: {
    position: 'absolute',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  },
  icon: {
    color: 'gray'
  }
};

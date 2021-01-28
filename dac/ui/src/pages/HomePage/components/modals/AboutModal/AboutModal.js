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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { get } from 'lodash';
import { FormattedMessage, injectIntl } from 'react-intl';
import Immutable from 'immutable';
import moment from 'moment';

import { formDescription } from 'uiTheme/radium/typography';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import metrics from '@app/metrics';

import Modal from 'components/Modals/Modal';
import Art from 'components/Art';
import Spinner from 'components/Spinner';

import { getEdition } from '@inject/utils/versionUtils';
import timeUtils from 'utils/timeUtils';

import TabsNavigationItem from '../../../../JobPage/components/JobDetails/TabsNavigationItem';
import {clusterData} from './AboutModal.less';

@injectIntl
export default class AboutModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.state = {
      inProgress: false,
      metricsInProgress: false,
      versionInfo: {},
      activeTab: 'build',
      tabs: this.tabDetails,
      users: [],
      jobs: [],
      average: {
        user: 0,
        job: 0
      }
    };
  }

  tabDetails =  Immutable.fromJS([
    {name: 'build', label: this.props.intl.formatMessage({ id: 'Common.BuildInfo'})},
    {name: 'metrics', label: this.props.intl.formatMessage({ id: 'Common.ClusterData'})}
  ])

  componentDidMount() {
    this.fetchInfo();
    this.getMetrics();
  }

  handleTabChange = (activeTab) => {
    this.setState({activeTab});
  };

  fetchInfo = () => {
    this.setState({
      inProgress: true
    });
    return ApiUtils.fetchJson('info', versionInfo => {
      this.setState({
        inProgress: false,
        versionInfo
      });
    }, () => this.setState({ inProgress: false }));
  };

  getMetrics = () => {
    this.setState({
      metricsInProgress: true
    }, async () => {
      const userData = await metrics.fetchUserStats();
      const jobData = await metrics.fetchDailyJobStats();
      const dates = this.getDates();
      const users = [];
      const jobs = [];
      const sum = {
        user: 0,
        job: 0
      };
      for (let i = 0; i < dates.length; i++) {
        if (userData.userStatsByDate) {
          const userList = userData.userStatsByDate.filter(item => item.date === dates[i]);
          if (userList.length !== 0) {
            users.push(userList[0].total);
            sum.user += userList[0].total;
          } else {
            users.push(0);
          }
        }
        if (jobData.jobStats) {
          const jobList = jobData.jobStats.filter(item => item.date === dates[i]);
          if (jobList.length !== 0) {
            jobs.push(jobList[0].total);
            sum.job += jobList[0].total;
          } else {
            jobs.push(0);
          }
        }
      }
      this.setState({
        users,
        jobs,
        average: {
          user: Math.round(sum.user / 7),
          job: Math.round(sum.job / 7)
        },
        metricsInProgress: false
      });
    });
  }

  renderTabs() {
    return (<div className='tabs-holder' style={styles.base}>
      {
        this.state.tabs.map(item => (
          <TabsNavigationItem
            item={item}
            key={item.get('name')}
            activeTab={this.state.activeTab}
            onClick={() => this.handleTabChange(item.get('name'))}>
            {item.get('label')}
          </TabsNavigationItem>
        ))
      }
    </div>);
  }

  renderTabsContent = (activeTab) => {
    return activeTab === 'build' ? this.renderVersion() : this.renderCluterData();
  };

  renderVersion() {
    const {
      versionInfo,
      inProgress
    } = this.state;

    const buildTime = timeUtils.formatTime(
      versionInfo.buildTime,
      la(timeUtils.INVALID_DATE_MSG),
      window.navigator.locale,
      timeUtils.formats.ISO
    );
    const commitTime = timeUtils.formatTime(
      get(versionInfo, 'commit.time'),
      la(timeUtils.INVALID_DATE_MSG),
      window.navigator.locale,
      timeUtils.formats.ISO
    );

    return inProgress ?
      <Spinner style={{top: 0}}/> :
      <dl className='normalFontSize'>
        <dt style={styles.dtStyle}><FormattedMessage id='App.Build'/></dt>
        <dd>{versionInfo.version}</dd>

        <dt style={styles.dtStyle}><FormattedMessage id='App.Edition'/></dt>
        <dd>{getEdition()}</dd>

        <dt style={styles.dtStyle}><FormattedMessage id='App.BuildTime'/></dt>
        <dd>{buildTime}</dd>

        <dt style={styles.dtStyle}><FormattedMessage id='App.ChangeHash'/></dt>
        <dd>{get(versionInfo, 'commit.hash')}</dd>

        <dt style={styles.dtStyle}><FormattedMessage id='App.ChangeTime'/></dt>
        <dd>{commitTime}</dd>
      </dl>;
  }

  getDates(format) {
    const dates = [];
    for (let i = 0; i < 7; i++) {
      dates.push(moment().subtract((i), 'days').format(format ? format : 'YYYY-MM-DD'));
    }
    return dates;
  }

  renderCluterData = () => {

    const {users, jobs, average, metricsInProgress} = this.state;

    return metricsInProgress ? <Spinner style={{top: 0}}/>  : (
      <table className={clusterData}>
        <thead>
          <tr>
            <td>Date</td>
            <td>Unique Users</td>
            <td>Jobs Executed</td>
          </tr>
        </thead>
        <tbody>
          {this.getDates('MM/DD/YYYY').map((item, i) => (
            <tr>
              <td>{item}</td>
              <td>{users[i] !== undefined ? users[i] : 0}</td>
              <td>{jobs[i] !== undefined ? jobs[i] : 0}</td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr>
            <td>Average</td>
            <td>{average.user}</td>
            <td>{average.job}</td>
          </tr>
        </tfoot>
      </table>
    );
  }

  render() {
    const { isOpen, hide, intl} = this.props;
    const { activeTab } = this.state;

    return (
      <Modal
        size='small'
        title={this.props.intl.formatMessage({id: 'App.AboutHeading'})}
        isOpen={isOpen}
        hide={hide}
      >
        <div style={styles.container}>
          <div style={styles.logoPane}>
            <Art className='dremioLogo' src='NarwhalLogo.svg' style={{width: 150}} alt={la('Dremio Narwhal')} />
          </div>
          <div style={styles.pane}>
            <div style={{flex: 1}}>
              <div style={{fontSize: '2em', marginBottom: 10}}>
                {intl.formatMessage({ id: 'App.Dremio' })}
              </div>
              {this.renderTabs()}
              <div>
                {this.renderTabsContent(activeTab)}
              </div>
            </div>
            <div style={formDescription}>
              {intl.formatMessage({ id: 'App.Copyright' })}
            </div>
          </div>
        </div>
      </Modal>
    );
  }
}

const styles = {
  container: {
    display: 'flex',
    width: '100%',
    height: '100%',
    padding: 20
  },
  pane: {
    flex: 2,
    marginLeft: '20px',
    display: 'flex',
    flexDirection: 'column'
  },

  logoPane: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    display: 'flex'
  },

  dtStyle: {
    fontWeight: '500',
    marginTop: 15,
    fontSize: '14px'
  },
  base: {
    height: 38,
    borderBottom: '1px solid #f3f3f3',
    display: 'flex'
  }
};

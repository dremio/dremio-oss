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
import Immutable from 'immutable';
import PropTypes from 'prop-types';

import { page } from '@app/uiTheme/radium/general';
import Message from '@app/components/Message';
import { SingleEngineInfoBar } from '@app/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineInfoBar';
import SingleEngineViewMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineViewMixin';
import { SINGLE_VIEW_TABS } from 'dyn-load/constants/provisioningPage/provisioningConstants';

@SingleEngineViewMixin
export class SingleEngineView extends Component {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map),
    queues: PropTypes.instanceOf(Immutable.List),
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  state = {
    activeTab: SINGLE_VIEW_TABS && SINGLE_VIEW_TABS.nodes,
    filterList: []
  };

  selectTab = (tab) => {
    this.setState({activeTab: tab});
  };

  renderErrorMessage = (engine) => {
    const message = engine && engine.get('error');
    if (!message) return null;

    return (
      <Message
        messageType='error'
        message={message}
        messageId={engine.get('id')}
        key={engine.get('id')}
        style={{ width: '100%' }}
      />
    );
  };

  render() {
    const { engine } = this.props;
    return (
      <div id='admin-engine' style={page}>
        {this.renderErrorMessage(engine)}
        <SingleEngineInfoBar engine={engine}/>
        {this.renderTabBar()}
        {this.renderTab()}
      </div>
    );
  }
}

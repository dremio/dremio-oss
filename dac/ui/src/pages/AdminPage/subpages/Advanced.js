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
import { connect } from 'react-redux';
import Immutable from 'immutable';

import { getDefinedSettings } from 'actions/resources/setting';
import { getViewState } from 'selectors/resources';

import ViewStateWrapper from 'components/ViewStateWrapper';
import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';
import Header from 'pages/AdminPage/components/Header';

import SettingsMicroForm from './SettingsMicroForm';
import { LABELS, SECTIONS } from './settingsConfig';

export const VIEW_ID = 'ADVANCED_SETTINGS_VIEW_ID';

export class Advanced extends PureComponent {
  static propTypes = {
    getAllSettings: PropTypes.func.isRequired,
    setChildDirtyState: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired
  };

  componentWillMount() {
    this.props.getAllSettings(Object.keys(LABELS), false, VIEW_ID); // all settings asumed to be loaded and used inside SettingsMicroForm
  }

  renderMicroForm(settingId) {
    const formKey = 'settings-' + settingId;
    return <SettingsMicroForm
      updateFormDirtyState={this.props.setChildDirtyState(formKey)}
      style={{marginTop: LABELS[settingId] !== '' ? 15 : 0}}
      form={formKey}
      key={formKey}
      settingId={settingId}
      viewId={VIEW_ID} />;
  }

  renderSections() {
    return Array.from(SECTIONS).map(([name, items]) => {
      return <div key={name} style={{padding: '10px 0 20px', borderBottom: '1px solid hsla(0, 0%, 0%, 0.1)'}}>
        <h3>{name}</h3>
        { Array.from(items).map(([settingId]) => this.renderMicroForm(settingId)) }
      </div>;
    });
  }

  render() {
    // SettingsMicroForm has a logic for error display. We should not duplicate it in the viewState
    const viewStateWithoutError = this.props.viewState.set('isFailed', false);
    return <div style={{display: 'flex', flexDirection: 'column', height: '100%'}}>
      <Header>{la('Queue Control')}</Header>
      <ViewStateWrapper viewState={viewStateWithoutError} style={{overflow: 'auto', height: '100%', flex: '1 1 auto'}}
        hideChildrenWhenFailed={false}>
        {this.renderSections()}
      </ViewStateWrapper>
    </div>;
  }

}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  getAllSettings: getDefinedSettings
})(FormUnsavedRouteLeave(Advanced));

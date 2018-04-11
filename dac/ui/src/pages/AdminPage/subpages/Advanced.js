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
import settingActions from 'actions/resources/setting';
import { addNotification } from 'actions/notification';

import { getViewState } from 'selectors/resources';
import Immutable from 'immutable';

import { formContext } from 'uiTheme/radium/typography';

import ViewStateWrapper from 'components/ViewStateWrapper';
import SimpleButton from 'components/Buttons/SimpleButton';
import TextField from 'components/Fields/TextField';
import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';

import Header from '../components/Header';
import SettingsMicroForm from './SettingsMicroForm';
import {LABELS, SECTIONS, LABELS_IN_SECTIONS} from './settingsConfig';

import {RESERVED as SUPPORT_RESERVED} from './Support';
const RESERVED = new Set([...SUPPORT_RESERVED]);

export const VIEW_ID = 'ADVANCED_SETTINGS_VIEW_ID';

export class Advanced extends PureComponent {
  static propTypes = {
    getAllSettings: PropTypes.func.isRequired,
    addNotification: PropTypes.func.isRequired,
    setChildDirtyState: PropTypes.func,

    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,

    settings: PropTypes.instanceOf(Immutable.Map).isRequired
  }

  state = {
    tempShown: new Immutable.OrderedSet()
  }

  componentWillMount() {
    this.props.getAllSettings({viewId: VIEW_ID});
  }

  settingExists(id) {
    return this.props.settings && this.props.settings.some(setting => setting.get('id') === id);
  }

  getShownSettings({includeSections = true} = {}) {
    const ret = !this.props.settings ? [] : this.props.settings.toList().toJS().filter(setting => {
      const id = setting.id;
      if (!includeSections && LABELS_IN_SECTIONS.hasOwnProperty(id)) return false;
      if (this.state.tempShown.has(id)) return true;
      if (RESERVED.has(id)) return false;
      return setting.showOutsideWhitelist;
    });

    return ret;
  }

  addAdvanced = (evt) => { // todo: replace with generic `prompt` modal
    evt.preventDefault();

    const value = evt.target.children[0].value;
    if (!value) {
      return;
    }

    const valueEle = <span style={{wordBreak: 'break-all'}}>{value}</span>;

    if (!this.settingExists(value)) {
      this.props.addNotification(
        <span>No setting “{valueEle}”.</span>, // todo: loc substitution engine
        'error'
      );
      return;
    }

    if (this.getShownSettings().some(e => e.id === value)) {
      this.props.addNotification(
        <span>Setting “{valueEle}” already shown.</span>, // todo: loc substitution engine
        'info'
      );
    } else {
      this.setState(function(state) {
        return {
          tempShown: state.tempShown.add(value)
        };
      });
    }

    evt.target.reset();
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

  sortSettings(settings) {
    const tempShownArray = this.state.tempShown.toArray();
    settings.sort((a, b) => {
      if (this.state.tempShown.has(a.id)) {
        if (this.state.tempShown.has(b.id)) {
          return tempShownArray.indexOf(b.id) - tempShownArray.indexOf(a.id);
        }
        return -1;
      } else if (this.state.tempShown.has(b.id)) {
        return 1;
      }

      if (LABELS.hasOwnProperty(a.id)) {
        if (LABELS.hasOwnProperty(b.id)) return LABELS[a.id] < LABELS[b.id] ? -1 : 1;
        return -1;
      } else if (LABELS.hasOwnProperty(b.id)) {
        return 1;
      }

      return a.id < b.id ? -1 : 1;
    });
  }

  renderOtherSettings() {
    const settings = this.getShownSettings({includeSections: false});

    this.sortSettings(settings);
    return settings.map(setting => this.renderMicroForm(setting.id));
  }

  renderSections() {
    return Array.from(SECTIONS).map(([name, items]) => {
      return <div key={name} style={{padding: '10px 0 20px', borderBottom: '1px solid hsla(0, 0%, 0%, 0.1)'}}>
        <h4>{name}</h4>
        { Array.from(items).map(([settingId]) => this.renderMicroForm(settingId)) }
      </div>;
    });
  }

  render() {
    const advancedForm = <form style={{flex: '0 0 auto'}} onSubmit={this.addAdvanced}>
      <TextField placeholder={la('Support Key')}/>
      <SimpleButton buttonStyle='secondary' style={{verticalAlign: 'bottom', width: 'auto'}}>{la('Show')}</SimpleButton>
    </form>;

    return <div style={{display: 'flex', flexDirection: 'column', height: '100%'}}>
      <Header>{la('Advanced Settings')}</Header>

      <ViewStateWrapper viewState={this.props.viewState} style={{overflow: 'auto', height: '100%', flex: '1 1 auto'}}>
        {this.renderSections()}
        <div style={{padding: '10px 0'}}>
          <div style={{display: 'flex', alignItems: 'center'}}>
            <h4 style={{flex: '1 1 auto'}}>{la('Dremio Support')}</h4>
            {advancedForm}
          </div>
          <div style={{...formContext}}>
            {la('Advanced settings provided by Dremio Support.')}
          </div>
          {this.renderOtherSettings()}
        </div>
      </ViewStateWrapper>
    </div>;
  }

}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
    settings: state.resources.entities.get('setting')
  };
}

export default connect(mapStateToProps, { // todo: find way to auto-inject PropTypes for actions
  getAllSettings: settingActions.getAll.dispatch,
  addNotification
})(FormUnsavedRouteLeave(Advanced));

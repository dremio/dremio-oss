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
import settingActions, { getDefinedSettings } from 'actions/resources/setting';
import { addNotification } from 'actions/notification';

import { getViewState } from 'selectors/resources';
import Immutable from 'immutable';
import { getErrorMessage } from '@app/reducers/resources/view';

import { description } from 'uiTheme/radium/forms';
import { formContext } from 'uiTheme/radium/typography';
import './Support.less';

import ViewStateWrapper from 'components/ViewStateWrapper';
import SimpleButton from 'components/Buttons/SimpleButton';
import TextField from 'components/Fields/TextField';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import SupportAccess,
  { RESERVED as SUPPORT_ACCESS_RESERVED } from 'dyn-load/pages/AdminPage/subpages/SupportAccess';
import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';

import Header from '../components/Header';
import SettingsMicroForm from './SettingsMicroForm';
import { LABELS, LABELS_IN_SECTIONS } from './settingsConfig';
import InternalSupportEmail, { RESERVED as INTERNAL_SUPPORT_RESERVED } from './InternalSupportEmail';

export const VIEW_ID = 'SUPPORT_SETTINGS_VIEW_ID';

export const RESERVED = new Set([...SUPPORT_ACCESS_RESERVED, ...INTERNAL_SUPPORT_RESERVED]);

export class Support extends PureComponent {
  static propTypes = {
    getDefinedSettings: PropTypes.func.isRequired,
    resetSetting: PropTypes.func.isRequired,
    addNotification: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    settings: PropTypes.instanceOf(Immutable.Map).isRequired,
    setChildDirtyState: PropTypes.func
  }

  componentWillMount() {
    this.props.getDefinedSettings([...RESERVED, ...LABELS_IN_SECTIONS], true, VIEW_ID);
  }

  state = {
    getSettingInProgress: false,
    tempShown: new Immutable.OrderedSet()
  }

  renderSettingsMicroForm = (settingId, props) => {
    const formKey = 'settings-' + settingId;
    return <SettingsMicroForm
      updateFormDirtyState={this.props.setChildDirtyState(formKey)}
      form={formKey}
      key={formKey}
      settingId={settingId}
      viewId={VIEW_ID}
      style={{margin: '5px 0'}}
      {...props} />;
  }


  settingExists(id) {
    return this.props.settings && this.props.settings.some(setting => setting.get('id') === id);
  }

  getShownSettings({includeSections = true} = {}) {
    const ret = !this.props.settings ? [] : this.props.settings.toList().toJS().filter(setting => {
      const id = setting.id;
      if (!includeSections && LABELS_IN_SECTIONS.hasOwnProperty(id)) return false;
      if (RESERVED.has(id)) return false;
      return true;
    });

    return ret;
  }

  addAdvanced = async (evt) => { // todo: replace with generic `prompt` modal
    evt.preventDefault();

    const value = evt.target.children[0].value;
    if (!value) {
      return;
    }

    const valueEle = <span style={{wordBreak: 'break-all'}}>{value}</span>;

    if (this.getShownSettings().some(e => e.id === value)) {
      this.props.addNotification(
        <span>Setting “{valueEle}” already shown.</span>, // todo: loc substitution engine
        'info'
      );
      return;
    }

    evt.persist(); // need to save an event as it used in async operation
    this.setState({
      getSettingInProgress: true
    });
    const reduxAction = await this.props.getSetting(value);
    const payload = reduxAction.payload;
    if (ApiUtils.isApiError(payload)) {
      this.props.addNotification(
        payload.status === 404 ? <span>No setting “{valueEle}”.</span> : // todo: loc substitution engine
          getErrorMessage(reduxAction).errorMessage,
        'error'
      );
    } else {
      this.setState(function(state) {
        return {
          tempShown: state.tempShown.add(value)
        };
      });
      evt.target.reset();
    }

    this.setState({
      getSettingInProgress: false
    });
  }

  resetSetting(settingId) {
    return this.props.resetSetting(settingId).then(() => {
      // need to remove the setting from our state
      this.setState(function(state) {
        return {
          tempShown: state.tempShown.delete(settingId)
        };
      });
    });
  }

  renderMicroForm(settingId, allowReset) {
    const formKey = 'settings-' + settingId;
    return <SettingsMicroForm
      updateFormDirtyState={this.props.setChildDirtyState(formKey)}
      style={{marginTop: LABELS[settingId] !== '' ? 15 : 0}}
      form={formKey}
      key={formKey}
      settingId={settingId}
      resetSetting={allowReset && this.resetSetting.bind(this, settingId)}
      viewId={VIEW_ID} />;
  }

  sortSettings(settings) {
    const orderedSet = this.state.tempShown;
    const tempShownArray = orderedSet.toArray();
    settings.sort((a, b) => {
      if (orderedSet.has(a.id)) {
        if (orderedSet.has(b.id)) {
          return tempShownArray.indexOf(b.id) - tempShownArray.indexOf(a.id);
        }
        return -1;
      } else if (orderedSet.has(b.id)) {
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
    return settings.map(setting => this.renderMicroForm(setting.id, true));
  }

  render() {
    // SettingsMicroForm has a logic for error display. We should not duplicate it in the viewState
    const viewStateWithoutError = this.props.viewState.set('isFailed', false);
    const advancedForm = <form style={{flex: '0 0 auto'}} onSubmit={this.addAdvanced}>
      <TextField placeholder={la('Support Key')} data-qa='support-key-search'/>
      <SimpleButton buttonStyle='secondary' data-qa='support-key-search-btn' submitting={this.state.getSettingInProgress}
        style={{verticalAlign: 'bottom', width: 'auto'}}>
        {la('Show')}
      </SimpleButton>
    </form>;

    return <div className='support-settings'>
      <Header>{la('Support Settings')}</Header>

      <ViewStateWrapper viewState={viewStateWithoutError} hideChildrenWhenFailed={false}>
        {!this.props.settings.size ? null : <div>
          {SupportAccess && <SupportAccess
            renderSettings={this.renderSettingsMicroForm}
            descriptionStyle={styles.description}
          />}
          <InternalSupportEmail
            renderSettings={this.renderSettingsMicroForm}
            descriptionStyle={styles.description}
          />
        </div>}
        <div style={{padding: '10px 0'}}>
          <div style={{display: 'flex', alignItems: 'center'}}>
            <h3 style={{flex: '1 1 auto'}}>{la('Support Keys')}</h3>
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
  resetSetting: settingActions.delete.dispatch,
  getSetting: settingActions.get.dispatch,
  getDefinedSettings,
  addNotification
})(FormUnsavedRouteLeave(Support));

const styles = {
  description: {
    ...description,
    margin: '5px 0'
  }
};

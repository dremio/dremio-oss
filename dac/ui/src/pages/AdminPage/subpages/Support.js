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

import { description } from 'uiTheme/radium/forms';
import './Support.less';

import ViewStateWrapper from 'components/ViewStateWrapper';
import SupportAccess,
  { RESERVED as SUPPORT_ACCESS_RESERVED } from 'dyn-load/pages/AdminPage/subpages/SupportAccess';
import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';

import Header from '../components/Header';
import SettingsMicroForm from './SettingsMicroForm';
import InternalSupportEmail, { RESERVED as INTERNAL_SUPPORT_RESERVED } from './InternalSupportEmail';

export const VIEW_ID = 'SUPPORT_SETTINGS_VIEW_ID';

export const RESERVED = new Set([...SUPPORT_ACCESS_RESERVED, ...INTERNAL_SUPPORT_RESERVED]);

export class Support extends PureComponent {
  static propTypes = {
    getAllSettings: PropTypes.func.isRequired,
    addNotification: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    settings: PropTypes.instanceOf(Immutable.Map).isRequired,
    setChildDirtyState: PropTypes.func
  }

  componentWillMount() {
    this.props.getAllSettings({viewId: VIEW_ID});
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

  render() {
    return <div className='support-settings'>
      <Header>{la('Support Settings')}</Header>

      <ViewStateWrapper viewState={this.props.viewState}>
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
})(FormUnsavedRouteLeave(Support));

const styles = {
  description: {
    ...description,
    margin: '5px 0'
  }
};

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
import { connect } from 'react-redux';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { loadUser } from 'actions/resources/user';
import { editAccount } from 'actions/account';
import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';
import ApiUtils from 'utils/apiUtils/apiUtils';

import Info from './Info';

@pureRender
export class InfoController extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired,
    username: PropTypes.string
  };

  static propTypes = {
    loadUser: PropTypes.func,
    route: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    editAccount: PropTypes.func
  }

  componentWillMount() {
    this.props.loadUser({userName: this.context.username});
  }

  submit = (form) => {
    const {userName, firstName, lastName, email, password, version } = form;
    const body = {
      userName: userName.value || userName.defaultValue,
      firstName: firstName.value || firstName.defaultValue,
      lastName: lastName.value || lastName.defaultValue,
      email: email.value || email.defaultValue,
      createdAt: new Date().getTime(), // todo: why is createdAt getting changed here?
      version: version.value,
      password: password.value || password.defaultValue
    };
    return ApiUtils.attachFormSubmitHandlers(
      this.props.editAccount(body, this.context.username)
    ).then(() => this.props.updateFormDirtyState(false));
  }

  cancel = () => {
    this.context.router.goBack();
  }

  render() {
    return (
      <Info
        updateFormDirtyState={this.props.updateFormDirtyState}
        onFormSubmit={this.submit}
        cancel={this.cancel}/>
    );
  }
}

export default connect(null, {
  loadUser,
  editAccount
})(FormUnsavedRouteLeave(InfoController));

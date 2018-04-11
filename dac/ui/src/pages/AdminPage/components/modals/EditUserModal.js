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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';

import Modal from 'components/Modals/Modal';
import { editUser } from 'actions/admin';
import { loadUser } from 'actions/resources/user';
import { getUser } from 'selectors/admin';
import ApiUtils from 'utils/apiUtils/apiUtils';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';

import EditUserForm from '../forms/EditUserForm';
import './Modal.less';

export class EditUserModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    loadUser: PropTypes.func,
    pathname: PropTypes.string.isRequired,
    editUser: PropTypes.func,
    query: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    user: PropTypes.instanceOf(Immutable.Map)
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  componentWillMount() {
    this.loadUserInfo(this.props);
  }

  componentWillReceiveProps(nextProps, nextContext) {
    if (this.props.isOpen !== nextProps.isOpen) {
      this.loadUserInfo(nextProps, nextContext);
    }
  }

  loadUserInfo(props, nextContext) {
    const { isOpen } = props;
    const user = nextContext ? nextContext.location.query.user : null;
    if (isOpen && user) {
      this.props.loadUser({userName: user});
    }
  }

  submit = (values) => {
    const { user } = this.props;
    const mappedValues = {
      userName: values.userName,
      firstName: values.firstName,
      lastName: values.lastName,
      version: values.version,
      email: values.email,
      createdAt: new Date().getTime(), // todo: why is createdAt getting changed here?
      password: values.password || undefined // undefined means no trying to set the pw to ""
    };
    return ApiUtils.attachFormSubmitHandlers(
      this.props.editUser(mappedValues, user.getIn(['userConfig', 'userName']))
    ).then(() => this.props.hide(null, true));
  }

  render() {
    const { isOpen, hide, user, updateFormDirtyState } = this.props;
    return (
      <Modal
        title={la('Edit User')}
        size='small'
        isOpen={isOpen}
        hide={hide}>
        <EditUserForm
          updateFormDirtyState={updateFormDirtyState}
          onFormSubmit={this.submit}
          onCancel={hide}
          user={user}/>
      </Modal>
    );
  }
}

function mapStateToProps(state) {
  const location = state.routing.locationBeforeTransitions;

  return {
    user: getUser(state, location.query.user)
  };
}

export default connect(mapStateToProps, {
  loadUser,
  editUser
})(FormUnsavedWarningHOC(EditUserModal));

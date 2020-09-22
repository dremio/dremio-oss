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
import { Component, Fragment } from 'react';
import Radium from 'radium';
import { createSelector } from 'reselect';
import { connect } from 'react-redux';
import { loadUser } from '@app/actions/modals/editUserModal';
import { DataLoader } from '@app/components/DataLoader';

import PropTypes from 'prop-types';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, modalFormProps } from 'components/Forms';
import UserForm from 'components/Forms/UserForm';
import usersDetails, { getUserInfo, moduleKey } from '@app/reducers/modules/usersDetails';
import { editUser } from '@app/actions/modals/editUserModal';
import { getModuleState } from '@app/reducers';
import { formBody } from 'uiTheme/less/forms.less';
import { moduleStateHOC } from '@app/containers/ModuleStateContainer';
import { compose } from 'redux';



const getPair = (formFieldName, entityFieldName) => ({
  form: formFieldName,
  entity: entityFieldName || formFieldName
});

const formFields = [
  getPair('userName', 'name'),
  getPair('firstName'),
  getPair('lastName'),
  getPair('tag'),
  getPair('email'),
  getPair('password'),
  getPair('extra')
];

const formToEntity = formFields.reduce((map, field) => {
  map[field.form] = field.entity;
  return map;
}, {});

const entityToForm = formFields.reduce((map, field) => {
  map[field.entity] = field.form;
  return map;
}, {});

const mapFields = (input, map) => {
  const result = {};

  for (const key in input) {
    if (input.hasOwnProperty(key) &&
      // skip a filed if there is no mapping info
      map[key]) {
      result[map[key]] = input[key];
    }
  }

  return result;
};

@Radium
export class EditUserForm extends Component {

  static propTypes = {
    userId: PropTypes.string,
    onCancel: PropTypes.func.isRequired,
    onFormSubmit: PropTypes.func.isRequired, // (submitPromise: Promise): void
    handleSubmit: PropTypes.func.isRequired,
    fields: PropTypes.object,
    passwordHasPadding: PropTypes.bool,
    isModal: PropTypes.bool,
    isReadMode: PropTypes.bool,
    updateFormDirtyState: PropTypes.func.isRequired, // required for FormDirtyStateWatcher

    //connected from pure redux
    editUser: PropTypes.func.isRequired
  };

  submit = (...args) => {
    const { handleSubmit, onFormSubmit, userId } = this.props;

    return handleSubmit(formValues => {
      const userConfig = mapFields(formValues, formToEntity);
      userConfig.id = userId;
      return onFormSubmit(
        this.props.editUser(userConfig)
      );
    })(...args);
  };

  render() {
    const { fields, passwordHasPadding, isModal, userId, isReadMode } = this.props;
    const form = <UserForm
      isReadMode={isReadMode}
      className={isModal ? formBody : null}
      passwordHolderStyles={passwordHasPadding ? styles.passwordHolder : null}
      fields={fields}
      noExtras
    />;
    return (
      <Fragment>
        <UserDetailLoader userId={userId} />
        {
          isReadMode ? form : (
            <ModalForm {...modalFormProps(this.props)} onSubmit={this.submit} isModal={isModal}>
              {form}
            </ModalForm>
          )
        }
      </Fragment>
    );
  }
}

const getInitialValues = createSelector(userConfig => userConfig, userConfig => mapFields(userConfig, entityToForm));


function mapStateToProps(state) {
  const userConfig = getUserInfo(getModuleState(state, moduleKey));

  if (userConfig) {
    return {
      userId: userConfig.id,
      initialValues: getInitialValues(userConfig)
    };
  }
}

export default compose(
  moduleStateHOC(moduleKey, usersDetails),
  connectComplexForm({
    form: 'editUser'
  }, [UserForm], mapStateToProps, {
    editUser
  })
)(EditUserForm);


@connect(null, {
  loadUser
})
class UserDetailLoader extends Component {
  static propTypes = {
    userId: PropTypes.string,

    //connected
    loadUser: PropTypes.func.isRequired
  };

  loadUserInfo = () => {
    const { userId } = this.props;

    if (userId) {
      this.props.loadUser(userId);
    }
  };

  render() {
    return <DataLoader keyValue={this.props.userId} onChange={this.loadUserInfo} />;
  }
}

const styles = {
  passwordHolder: {
    margin: '40px 0 0'
  }
};

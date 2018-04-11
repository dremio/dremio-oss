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
import React, { Component } from 'react';
import Radium from 'radium';

import PropTypes from 'prop-types';

import { getUser } from 'selectors/admin';
import config from 'utils/config';

import { FLEX_WRAP_COL_START, LINE_START_CENTER, LINE_NOWRAP_BETWEEN} from 'uiTheme/radium/flexStyle';
import { formLabel } from 'uiTheme/radium/typography';
import { PALE_GREY, SECONDARY, SECONDARY_BORDER } from 'uiTheme/radium/colors';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import UserForm, { userFormValidate, userFormFields } from 'components/Forms/UserForm';
import FormInfo, {modalFormProps} from './FormInfo';
import './Info.less';

@Radium
export class Info extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    fields: PropTypes.object,
    cancel: PropTypes.func
  };

  render() {
    const { onFormSubmit, handleSubmit } = this.props;

    let view = <UserForm fields={this.props.fields} style={{padding: 0}}/>;
    if (!config.showUserAndUserProperties) {
      view = React.cloneElement(view, { isReadMode: true });
    } else {
      view = (
        <FormInfo {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit.bind(this, this.props.fields))}>
          {view}
        </FormInfo>
      );
    }

    return (
      <div className='account-info-form'>
        <h2 style={styles.header}>{la('General Information')}</h2>
        {view}
      </div>
    );
  }
}

function mapStateToProps(state) {
  const user = getUser(state, state.account.getIn(['user', 'userName']));
  if (user) {
    const userConfig = user.get('userConfig');
    return {
      initialValues: {
        ...userConfig.toJS(),
        newPassword: '',
        confirmPass: ''
      }
    };
  }
}

export default connectComplexForm({
  form: 'accountInfo',
  fields: userFormFields,
  validate: userFormValidate
}, [], mapStateToProps)(Info);

const styles = {
  header: {
    borderBottom: `2px solid ${PALE_GREY}`,
    marginBottom: 30,
    paddingBottom: 10
  },
  wrap: {
    ...FLEX_WRAP_COL_START,
    marginTop: 20,
    width: 700
  },
  wrapBox: {
    ...FLEX_WRAP_COL_START,
    margin: '15px 0 30px 0'
  },
  input: {
    ...FLEX_WRAP_COL_START,
    ...formLabel,
    marginTop: 15

  },
  avatarBox: {
    ...LINE_NOWRAP_BETWEEN,
    width: 150,
    height: 50,
    borderRadius: 2,
    border: '1px solid rgba(0,0,0,0.10)'
  },
  avatarIcon: {
    margin: '5px 0 0 5px',
    height: 40,
    width: 40
  },
  editor: {
    ...LINE_START_CENTER,
    paddingLeft: 5,
    width: 100,
    height: 50,
    ':hover': {
      backgroundColor: SECONDARY,
      borderBottom: `1px ${SECONDARY_BORDER}`,
      cursor: 'pointer'
    }
  }
};

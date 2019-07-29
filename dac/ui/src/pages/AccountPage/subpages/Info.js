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
import { connect } from 'react-redux';

import PropTypes from 'prop-types';
import config from 'dyn-load/utils/config';
import EditUserForm from '@app/pages/AdminPage/components/forms/EditUserForm';
import { modalFormProps } from '@app/components/Forms';
import Header from '@app/pages/AdminPage/components/Header';

import './Info.less';


export class Info extends Component {
  static propTypes = {
    userId: PropTypes.string,
    onFormSubmit: PropTypes.func.isRequired,
    updateFormDirtyState: PropTypes.func.isRequired,
    cancel: PropTypes.func
  };

  render() {
    const { onFormSubmit, userId, cancel, updateFormDirtyState } = this.props;

    const addProps = modalFormProps(this.props);
    return (
      <div className='account-info-form'>
        <Header style={styles.header}>
          {la('General Information')}
        </Header>
        <EditUserForm
          userId={userId}
          {...addProps}
          onCancel={cancel}
          onFormSubmit={onFormSubmit}
          updateFormDirtyState={updateFormDirtyState}
          isModal={false}
          isReadMode={!config.showUserAndUserProperties}
        />
      </div>
    );
  }
}

function mapStateToProps(state) {
  const props = {
    userId: state.account.getIn(['user', 'userId'])
  };

  return props;
}

export default connect(mapStateToProps)(Info);

const styles = {
  header: {
    marginBottom: 30
  }
};

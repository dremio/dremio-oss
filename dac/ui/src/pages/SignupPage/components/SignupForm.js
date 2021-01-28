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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import Spinner from '@app/components/Spinner';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { createFirstUser } from 'actions/admin';
import { noUsersError } from 'actions/account';
import { getViewState } from 'selectors/resources';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm.js';
import { divider, formRow } from 'uiTheme/radium/forms';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

import UserForm from 'components/Forms/UserForm';

import SignupTitle from './SignupTitle';

export const SIGNUP_FORM_VIEW_ID = 'SIGNUP_FORM_VIEW_ID';

@Radium
@pureRender
export class SignupForm extends Component {
  static propTypes = {
    createFirstUser: PropTypes.func,
    fields: PropTypes.object,
    viewState: PropTypes.instanceOf(Immutable.Map),
    noUsersError: PropTypes.func,
    location: PropTypes.object.isRequired
  }

  state = {
    showSpinner: false
  }

  componentDidMount() {
    this.props.noUsersError(); // if user navigated directly to /signup - ensure socket closing, etc
  }

  submit = (form) => {
    const instanceId = localStorageUtils.getInstanceId();
    const mappedValues = {
      'userName' : form.userName,
      'firstName' : form.firstName,
      'lastName' : form.lastName,
      'email' : form.email,
      'createdAt' : new Date().getTime(),
      'password': form.password,
      'extra': form.extra || instanceId
    };
    const viewId = SIGNUP_FORM_VIEW_ID;
    this.setState({showSpinner: true}, () => {
      return this.props.createFirstUser(mappedValues, {viewId});
    });
  }

  render() {
    const { viewState, fields } = this.props;

    return (
      <div id='signup-form' style={[styles.base]}>
        <SignupTitle />
        <ViewStateWrapper viewState={viewState} />
        <InnerComplexForm
          {...this.props}
          style={styles.form}
          onSubmit={this.submit}>
          <UserForm
            fields={fields}
            style={{padding: 0}}
          />
          <hr style={[divider, { width: '100vw'}]}/>
          <div style={styles.footer}>
            <div style={styles.submit}>
              <Button
                type={ButtonTypes.NEXT}
                text={la('Next')}
                disable={this.state.showSpinner}
              />
              { this.state.showSpinner &&
                <Spinner
                  iconStyle={styles.spinnerIcon}
                  style={styles.spinner}
                  message='Loading...'
                />
              }
            </div>
            <div style={styles.footerLink}>
              <a href='https://www.dremio.com/legal/privacy-policy' target='_blank'>{la('Privacy')}</a>
            </div>
          </div>
        </InnerComplexForm>
      </div>
    );
  }
}

const styles = {
  base: {
    width: 640
  },
  form: {
    display: 'flex',
    flexWrap: 'wrap'
  },
  formRow: {
    ...formRow,
    display: 'flex'
  },
  formRowSingle: {
    ...formRow,
    display: 'flex',
    width: '100%'
  },
  footer: {
    display: 'flex',
    width: '100%',
    alignItems: 'center',
    justifyContent: 'space-between'
  },
  footerLink: {
    marginBottom: '5px'
  },
  spinner: {
    position: 'relative',
    height: 'auto',
    width: 'auto'
  },
  spinnerIcon: {
    width: 24,
    height: 24
  },
  submit: {
    display: 'flex',
    alignItems: 'center'
  }
};

function mapToFormState(state) {
  return {
    viewState: getViewState(state, SIGNUP_FORM_VIEW_ID)
  };
}

export default connectComplexForm({
  form: 'signup'
}, [UserForm], mapToFormState, { createFirstUser, noUsersError })(SignupForm);

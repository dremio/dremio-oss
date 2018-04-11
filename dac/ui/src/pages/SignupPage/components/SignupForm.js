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
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { createFirstUser } from 'actions/admin';
import { noUsersError } from 'actions/account';
import { getViewState } from 'selectors/resources';
import { InnerComplexForm, connectComplexForm } from 'components/Forms/connectComplexForm.js';
import { divider, formRow } from 'uiTheme/radium/forms';
import { Link } from 'react-router';

import UserForm, { userFormFields, userFormValidate } from 'components/Forms/UserForm';

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

  componentDidMount() {
    this.props.noUsersError(); // if user navigated directly to /signup - ensure socket closing, etc
  }

  submit = (form) => {
    const mappedValues = {
      'userName' : form.userName,
      'firstName' : form.firstName,
      'lastName' : form.lastName,
      'email' : form.email,
      'createdAt' : new Date().getTime(),
      'password': form.password
    };
    const viewId = SIGNUP_FORM_VIEW_ID;
    return this.props.createFirstUser(mappedValues, {viewId});
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
            <Button
              type={ButtonTypes.NEXT}
              text={la('Next')}
            />
            <div className='largerFontSize'>
              {<Link to={{ ...this.props.location, state: { modal: 'AboutModal' }}}>
                {la('About Dremio')}
              </Link>}
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
  }
};

function mapToFormState(state) {
  return {
    viewState: getViewState(state, SIGNUP_FORM_VIEW_ID)
  };
}

export default connectComplexForm({
  form: 'signup',
  fields: userFormFields,
  validate: userFormValidate
}, [], mapToFormState, { createFirstUser, noUsersError })(SignupForm);

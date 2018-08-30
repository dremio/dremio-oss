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
import Radium from 'radium';
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import { replace } from 'react-router-redux';
import Immutable from 'immutable';
import urlParse from 'url-parse';

import { loginUser } from 'actions/account';
import { applyValidators, isRequired } from 'utils/validation';
import Spinner from 'components/Spinner';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import { InnerComplexForm, connectComplexForm } from 'components/Forms/connectComplexForm';
import FieldWithError from 'components/Fields/FieldWithError.js';
import TextField from 'components/Fields/TextField.js';
import { getViewState } from 'selectors/resources';
import ViewStateWrapper from 'components/ViewStateWrapper';

import { formLabel, lightLink } from 'uiTheme/radium/typography';

import LoginTitle from './LoginTitle';

const VIEW_ID = 'LoginForm';

@Radium
export class LoginForm extends Component {
  static propTypes = {
    user: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    viewState: PropTypes.instanceOf(Immutable.Map),
    loginUser: PropTypes.func.isRequired,
    replace: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired
  };

  static validate(values) {
    return applyValidators(values, [isRequired('userName', 'Username'), isRequired('password')]);
  }

  submit = (form) => {
    return this.props.loginUser(form, this.props.viewState.get('viewId')).then((response) => {
      if (response && !response.error) {
        let url = '/';
        if (typeof window !== 'undefined') {
          const parsedUrl = urlParse(window.location.href, true);
          url = (parsedUrl.query && parsedUrl.query.redirect) || url;
        }
        this.props.replace(url);
      }
    });
  }

  render() {
    const { fields: { userName, password }, viewState, location } = this.props;

    return (
      <div id='login-form' style={[styles.base]}>
        <LoginTitle
          style={{marginBottom: 10}}
          subTitle={la('Welcome to Dremio, please log in.')}/>
        <ViewStateWrapper
          style={{paddingTop: 45}}
          hideChildrenWhenFailed={false}
          viewState={viewState}
          hideSpinner>
          <InnerComplexForm
            {...this.props}
            style={styles.form}
            onSubmit={this.submit}>
            <div style={styles.fieldsRow}>
              <FieldWithError
                {...userName}
                errorPlacement='top'
                label={la('Username')}
                labelStyle={styles.label}
                style={{...formLabel, ...styles.field}}>
                <TextField
                  {...userName}
                  initialFocus
                  style={styles.input}/>
              </FieldWithError>
              <FieldWithError
                {...password}
                errorPlacement='top'
                label={la('Password')}
                labelStyle={styles.label}
                style={{...formLabel, ...styles.field}}>
                <TextField
                  {...password}
                  type='password'
                  style={styles.input}/>
              </FieldWithError>
            </div>
            <div style={styles.submitWrapper}>
              <Button
                type={ButtonTypes.NEXT}
                key='details-wizard-next'
                style={{marginBottom: 0}}
                text={la('Log In')}/>
              <Spinner
                iconStyle={styles.spinnerIcon}
                style={{display: viewState.get('isInProgress') ? 'block' : 'none', ...styles.spinner}}/>
              {false && <div style={styles.link}>
                <Link to='#' >{la('Forgot your password?')}</Link>
              </div>}
            </div>
          </InnerComplexForm>
        </ViewStateWrapper>
        <div className='largerFontSize' style={{textAlign: 'right'}}>
          <Link to={{ ...location, state: { modal: 'AboutModal' }}}>
            {la('About Dremio')}
          </Link>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connectComplexForm({
  form: 'login',
  validate: LoginForm.validate,
  fields: ['userName', 'password']
}, [], mapStateToProps, {
  loginUser,
  replace
})(LoginForm);

const styles = {
  base: {
    position: 'relative',
    backgroundColor: '#344253',
    minWidth: 775,
    height: 430,
    maxWidth: 775,
    maxHeight: 430,
    overflow: 'hidden',
    padding: 40,
    display: 'flex',
    flexDirection: 'column'
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
  label: {
    color: '#fff',
    marginBottom: 3
  },
  fieldsRow: {
    display: 'flex',
    flexDirection: 'row',
    justifyContent: 'space-between'
  },
  field: {
    flexBasis: 'calc(50% - 5px)'
  },
  input: {
    width: '100%',
    marginRight: 0
  },
  submitWrapper: {
    margin: '20px 0 0 0',
    display: 'flex',
    flexDirection: 'row'
  },
  link: {
    flexGrow: 1,
    textAlign: 'right',
    alignSelf: 'center',
    ...lightLink
  },
  form: {
    display: 'flex',
    flexWrap: 'wrap',
    flexDirection: 'column',
    justifyContent: 'space-between',
    width: 695
  }
};

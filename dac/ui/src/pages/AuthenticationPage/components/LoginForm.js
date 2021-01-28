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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';

import { LOGIN_VIEW_ID, loginUser } from '@inject/actions/account';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from '@app/components/Buttons/Button';
import { FieldWithError, TextField } from '@app/components/Fields';
import { getViewState } from 'selectors/resources';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';
import ViewStateWrapper from 'components/ViewStateWrapper';
import Spinner from '@app/components/Spinner';

import { formLabel } from 'uiTheme/radium/typography';
import { applyValidators, isRequired } from '@app/utils/validation';

@Radium
export class LoginForm extends PureComponent {
  static propTypes = {
    showMessage: PropTypes.bool,
    // redux-form
    fields: PropTypes.object,
    // connected
    user: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    loginUser: PropTypes.func.isRequired
  };

  static defaultProps = {
    showMessage: true
  };

  static validate(values) {
    return applyValidators(values, [isRequired('userName', 'Username'), isRequired('password')]);
  }

  submit = (form) => {
    const {
      loginUser: dispatchLoginUser,
      viewState
    } = this.props;
    return dispatchLoginUser(form, viewState.get('viewId'));
  }

  render() {
    const {
      fields: {
        userName,
        password
      },
      showMessage,
      viewState
    } = this.props;
    return (
      <ViewStateWrapper
        style={{paddingTop: 30}}
        hideChildrenWhenFailed={false}
        viewState={viewState}
        showMessage={showMessage}
        hideSpinner
        multilineErrorMessage
      >
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
            <div style={{display: 'flex', flexGrow: 1}}>
              <Button
                type={ButtonTypes.NEXT}
                key='details-wizard-next'
                style={{marginBottom: 0}}
                text={la('Log In')}/>
              <Spinner
                iconStyle={styles.spinnerIcon}
                style={{display: viewState.get('isInProgress') ? 'block' : 'none', ...styles.spinner}}/>
            </div>
            <div style={{display: 'flex', alignItems: 'center'}}>
              <a href='https://www.dremio.com/legal/privacy-policy' target='_blank'>{la('Privacy')}</a>
            </div>
          </div>
        </InnerComplexForm>
      </ViewStateWrapper>
    );
  }
}

const styles = {
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
    flexBasis: 'calc(50% - 5px)',
    display: 'block'
  },
  input: {
    width: '100%',
    marginRight: 0
  },
  submitWrapper: {
    margin: '55px 0 0 0',
    display: 'flex',
    flexDirection: 'row'
  },
  form: {
    display: 'flex',
    flexWrap: 'wrap',
    flexDirection: 'column',
    justifyContent: 'space-between',
    width: 695
  }
};

function mapStateToProps(state) {
  return {
    user: state.account.get('user'),
    viewState: getViewState(state, LOGIN_VIEW_ID)
  };
}

export default  connectComplexForm({
  form: 'login',
  fields: ['userName', 'password']
}, [LoginForm], mapStateToProps, {
  loginUser
})(LoginForm);

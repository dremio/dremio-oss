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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import Message from 'components/Message';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';


import { FLEX_WRAP_COL_START } from 'uiTheme/radium/flexStyle';

export function modalFormProps(props) {
  return {
    cancel: props.cancel,
    error: props.error,
    submitting: props.submitting
  };
}

@pureRender
@Radium
export default class FormInfo extends Component {
  static propTypes = {
    onSubmit: PropTypes.func.isRequired,
    cancel: PropTypes.func.isRequired,
    error: PropTypes.shape({
      id: PropTypes.string,
      message: PropTypes.instanceOf(Immutable.Map)
    }),
    submitting: PropTypes.bool,
    children: PropTypes.node.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {onSubmit, cancel, error, submitting, children} = this.props;
    return (
      <form onSubmit={onSubmit} style={styles.base}>
        {
          error
            ? <Message
              messageType='error'
              message={error.message}
              messageId={error.id}
              detailsStyle={{maxHeight: 100}}
              style={styles.message}/>
            : null
        }
        {children}
        <ConfirmCancelFooter modalFooter={false} submitting={submitting} confirm={onSubmit} cancel={cancel} />
      </form>
    );
  }
}

const styles = {
  base: {
    ...FLEX_WRAP_COL_START,
    width: 640
  },
  message: {
    marginBottom: 10,
    paddingRight: 10
  }
};

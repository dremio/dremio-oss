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
import { Component, Fragment } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import { SelectView } from '@app/components/Fields/SelectView';
import { triangleTop } from 'uiTheme/radium/overlay';
import AdjustWorkersForm  from './forms/AdjustWorkersForm';

export default class AdjustWorkers extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    readonly: PropTypes.bool
  }

  render() {
    const { entity, readonly } = this.props;
    return (
      <SelectView
        style={styles.fullWidth}
        content={
          <Button
            disable={readonly}
            style={styles.fullWidth}
            type={ButtonTypes.NEXT}
            text={la('Add / Remove Workers')}
          />
        }
        listStyle={styles.popover}
        hideExpandIcon
        useLayerForClickAway={false}
        disabled={readonly}
      >
        {
          ({ closeDD }) => (
            <Fragment>
              <div style={styles.triangle}/>
              <h4 style={styles.formTitle}>{la('Workers')}</h4>
              <AdjustWorkersForm
                onCancel={closeDD}
                ref='dropDown'
                entity={entity}
                parent={this}/>
            </Fragment>
          )
        }

      </SelectView>
    );
  }
}

const styles = {
  fullWidth: {
    width: '100%'
  },
  formTitle: {
    paddingTop: 10,
    paddingLeft: 10
  },
  popover: {
    overflow: 'visible',
    marginTop: 7,
    width: 280
  },
  triangle: {
    ...triangleTop,
    left: 6
  }
};

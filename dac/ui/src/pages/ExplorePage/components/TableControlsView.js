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
import classNames from 'classnames';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import { Link } from 'react-router';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import { injectIntl } from 'react-intl';

import TableControlsMenu from 'components/Menus/ExplorePage/TableControlsMenu';
import TableControlsViewMixin from 'dyn-load/pages/ExplorePage/components/TableControlsViewMixin';

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';

import { PALE_BLUE } from 'uiTheme/radium/colors.js';
import { sqlEditorButton } from 'uiTheme/radium/buttons';

import './TableControls.less';
import SqlToggle from './SqlEditor/SqlToggle';
import SampleDataMessage from './SampleDataMessage';

@injectIntl
@pureRender
@Radium
@TableControlsViewMixin
class TableControls extends Component {
  static contextTypes = {
    location: PropTypes.object
  };

  static propTypes = {
    isGraph: PropTypes.bool,
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    columns: PropTypes.instanceOf(Immutable.List),
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,

    closeDropdown: PropTypes.func.isRequired,
    toogleDropdown: PropTypes.func.isRequired,
    dataGraph: PropTypes.func.isRequired,
    groupBy: PropTypes.func.isRequired,
    addField: PropTypes.func,
    handleRequestClose: PropTypes.func.isRequired,
    join: PropTypes.func.isRequired,
    sqlState: PropTypes.bool.isRequired,
    dropdownState: PropTypes.bool.isRequired,
    anchorEl: PropTypes.object,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);

    this.state = {
      tooltipState: false,
      anchorOrigin: {
        horizontal: 'right',
        vertical: 'bottom'
      },
      targetOrigin: {
        horizontal: 'right',
        vertical: 'top'
      }
    };
  }

  getVisibleColumnsLabel(columns = Immutable.List()) {
    const visibleColumns = columns.filter(column => !column.get('hidden'));
    return `${visibleColumns.size} of ${columns.size}`;
  }

  renderPreviewWarning() {
    const { approximate } = this.props;
    return approximate && <SampleDataMessage />;
  }

  render() {
    const { columns, isGraph, rightTreeVisible, dataset, intl } = this.props;
    const { location } = this.context;
    const classWrap = classNames('control-wrap', { 'inactive-element': isGraph });
    const toggleButton = !this.props.sqlState
      ? (
        <SqlToggle dataset={dataset}/>
      )
      : null;
    const disable = this.props.exploreViewState.get('isInProgress');
    const width = rightTreeVisible ? { width: 'calc(100% - 251px)' } : {};

    return (
      <div className='table-controls' style={[styles.tableControls, disable && styles.disabledStyle, width]}>
        <div className='left-controls'>
          <div className='controls' style={styles.controlsInner}>
            {toggleButton}
            <Button
              key='calc'
              innerText={styles.innerTextStyle}
              type={ButtonTypes.SECONDARY} icon='AddFields'
              text={intl.formatMessage({ id: 'Dataset.AddField' })}
              styles={{...sqlEditorButton, ...styles.activeButton}}
              iconStyle={{ Container: styles.iconContainer, Icon: styles.iconBox }}
              onClick={this.props.addField}/>
            <Button
              key='groupBy'
              innerText={{...styles.innerTextStyle}}
              type={ButtonTypes.SECONDARY} icon='GroupBy'
              text={intl.formatMessage({ id: 'Dataset.GroupBy' })}
              styles={{...sqlEditorButton, ...styles.activeButton}}
              iconStyle={{ Container: {...styles.iconContainer}, Icon: styles.iconBox }}
              onClick={this.props.groupBy}/>
            <Button
              key='join'
              innerText={styles.innerTextStyle}
              type={ButtonTypes.SECONDARY} icon='Join'
              text={intl.formatMessage({ id: 'Dataset.Join' })}
              styles={{...sqlEditorButton, ...styles.activeButton}}
              iconStyle={{ Container: styles.iconContainer, Icon: styles.iconBox }}
              onClick={this.props.join}/>
            {this.renderPreviewWarning()}
          </div>
        </div>
        <div className='right-controls'>
          <div className='controls'>
            <div className={classWrap}>
              <span>{`${intl.formatMessage({ id: 'Dataset.Fields' })}:\u00a0`}</span>
              <span style={{ color: '#52b8d8' }}>
                <Link
                  className='separator-line columns-text'
                  data-qa='edit-columns-link'
                  to={{ ...location, state: { modal: 'EditColumnsModal' }}}>
                  {this.getVisibleColumnsLabel(columns)}
                </Link>
              </span>
            </div>
            {this.renderGraphButton()}
          </div>
        </div>
        <Popover
          useLayerForClickAway={false}
          open={this.props.dropdownState}
          canAutoPosition
          anchorEl={this.props.anchorEl}
          anchorOrigin={this.state.anchorOrigin}
          targetOrigin={this.state.targetOrigin}
          onRequestClose={this.props.handleRequestClose}
          animation={PopoverAnimationVertical}>
          <div style={styles.popover}>
            <TableControlsMenu />
          </div>
        </Popover>
      </div>
    );
  }
}

export const styles = {
  disabledStyle: {
    pointerEvents: 'none',
    opacity: 0.7
  },
  innerTextStyle: {
    top: '-7px',
    textAlign: 'left'
  },
  activeButton: {
    ':hover': {
      backgroundColor: 'rgb(229, 242, 247)'
    },
    color: 'rgb(0, 0, 0)'
  },
  iconBox: {
    width: 24,
    height: 24
  },
  iconContainer: {
    marginRight: 1,
    lineHeight: '24px',
    width: 24,
    position: 'relative'
  },
  tableControls: {
    marginTop: 0,
    marginLeft: 0,
    paddingLeft: 5,
    height: 42,
    display: 'flex',
    alignItems: 'center',
    backgroundColor: PALE_BLUE
  },
  controlsInner: {
    height: 24
  }
};

export default TableControls;

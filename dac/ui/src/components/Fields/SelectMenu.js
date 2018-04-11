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
import { Popover } from 'material-ui/Popover';
import Immutable from 'immutable';

import FontIcon from 'components/Icon/FontIcon';
import { formDefault } from 'uiTheme/radium/typography';

// todo: DEPRECATED: use Select instead

@pureRender
@Radium
export default class SelectMenu extends Component {

  static propTypes = {
    items: PropTypes.instanceOf(Immutable.List), // [{label: string, value: string, icon: component}]
    name: PropTypes.string,
    selectedItem: PropTypes.string,
    onItemSelect: PropTypes.func,
    label: PropTypes.string,
    shouldShowSelectedInTitle: PropTypes.bool,
    hideSelectedLabel: PropTypes.bool
  };

  constructor(props) {
    super(props);
    this.handleRequestOpen = this.handleRequestOpen.bind(this);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.handleItemSelect = this.handleItemSelect.bind(this);
    this.state = {
      isPopoverOpened: false,
      anchorEl: null,
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

  handleItemSelect(value) {
    const itemToAdd = Immutable.fromJS(this.props.items.find(item => item.id === value));
    if (this.props.onItemSelect) {
      this.props.onItemSelect(value, itemToAdd.get('filterName'));
    }
  }

  handleRequestClose() {
    this.setState({
      isPopoverOpened: false
    });
  }

  handleRequestOpen() {
    this.setState({
      isPopoverOpened: true,
      anchorEl: this.refs.target
    });
  }

  renderInfoAboutSelectedItems() {
    if (!this.props.selectedItem || this.props.hideSelectedLabel) {
      return null;
    }
    const item = this.props.items.find(it => this.props.selectedItem === it.get('id'));
    return <span>: {item && item.get('label')}</span>;
  }

  renderItems() {
    return this.props.items.map(item => {
      return (
        <div
          data-qa={`${item.get('id')}-filter-item`}
          style={[styles.item]}
          key={item.get('id')}
          onClick={this.props.onItemSelect.bind(this, item.get('id'))}>
          {item.get('label')}
        </div>
      );
    });
  }

  render() {
    const { label, name } = this.props;
    return (
      <div
        ref='target'
        data-qa={`${name}-filter`}
        onClick={this.handleRequestOpen}
        style={[styles.base]}
        className='filter-select-menu field'>
        <span>{label}</span>
        {this.renderInfoAboutSelectedItems()}
        <FontIcon type='ArrowDownSmall'/>
        <Popover
          style={styles.popover}
          useLayerForClickAway
          canAutoPosition
          open={this.state.isPopoverOpened}
          anchorEl={this.state.anchorEl}
          anchorOrigin={this.state.anchorOriginType}
          targetOrigin={this.state.targetOriginType}
          onRequestClose={this.handleRequestClose}
        >
          <div style={styles.popoverContent}>
            {this.renderItems()}
          </div>
        </Popover>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    marginRight: 10,
    cursor: 'pointer'
  },
  popoverContent: {
    display: 'flex',
    flexWrap: 'wrap',
    flexDirection: 'column',
    minWidth: 100
  },
  item: {
    ...formDefault,
    display: 'flex',
    padding: '8px 24px',
    cursor: 'pointer',
    ':hover': {
      backgroundColor: 'rgba(0, 0, 0, 0.0980392)'
    }
  }
};

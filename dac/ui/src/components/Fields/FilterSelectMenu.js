/*
 * Copyright (C) 2017 Dremio Corporation
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
import { injectIntl } from 'react-intl';

import EllipsedText from 'components/EllipsedText';

import FontIcon from 'components/Icon/FontIcon';
import Checkbox from 'components/Fields/Checkbox';
import { SearchField } from 'components/Fields';

FilterSelectMenuItem.propTypes = {
  item: PropTypes.object.isRequired,
  onChange: PropTypes.func.isRequired,
  checked: PropTypes.bool
};

export function FilterSelectMenuItem({item, onChange, checked}) {
  return (<div style={{display: 'flex', paddingLeft: 7}} key={item.id}>
    <Checkbox
      onChange={() => onChange(checked, item.id)}
      label={[
        item.icon && <FontIcon type={item.icon} theme={{ Container: { overflow: 'hidden', height: 24, width: 24 }}}/>,
        item.label
      ]}
      checked={checked}
      dataQa={item.id + '-filter-item'}
    />
  </div>);
}

@injectIntl
@pureRender
@Radium
export default class FilterSelectMenu extends Component {

  static propTypes = {
    items: PropTypes.array, // [{label: string, id: string, icon: component}]
    hideOnSelect: PropTypes.bool,
    selectedValues: PropTypes.instanceOf(Immutable.List),
    name: PropTypes.string,
    label: PropTypes.string,
    searchPlaceholder: PropTypes.string,

    preventSelectedLabel: PropTypes.bool,
    noSearch: PropTypes.bool,
    selectedToTop: PropTypes.bool,

    // callbacks
    loadItemsForFilter: PropTypes.func,
    onItemSelect: PropTypes.func,
    onItemUnselect: PropTypes.func,

    intl: PropTypes.object.isRequired
  };

  static defaultProps = { // todo: `la` loc not building correctly here
    items: [],
    selectedValues: Immutable.List(),
    searchPlaceholder: ('Search')
  };

  constructor(props) {
    super(props);
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
    this.updateValueIsSelected(props);
  }

  componentWillReceiveProps(nextProps) {
    this.updateValueIsSelected(nextProps);
  }

  getSelectedItems() {
    const { items } = this.props;
    return items.filter(
      item => this.valueIsSelected[item.id]
    );
  }

  getUnselectedItems() {
    const { items } = this.props;
    const { pattern } = this.state;
    return items.filter(
      item => !this.valueIsSelected[item.id]
      && (!pattern || item.label.toLowerCase().indexOf(pattern.trim().toLowerCase()) === 0)
    );
  }

  updateValueIsSelected(props) {
    this.valueIsSelected = props.items.reduce((prev, item) => {
      prev[item.id] = props.selectedValues.includes(item.id);
      return prev;
    }, {});
  }

  handleSearchForItem = (value) => {
    this.setState({
      pattern: value
    });

    // TODO uncomment when API is done
    // if (this.props.loadItemsForFilter) {
    //   this.props.loadItemsForFilter(value, 100);
    // }
  }

  handleItemChange = (checked, id) => {
    if (checked) {
      if (this.props.onItemUnselect) {
        this.props.onItemUnselect(id);
      }
    } else if (this.props.onItemSelect) {
      this.props.onItemSelect(id);
    }
    if (this.props.hideOnSelect) {
      this.handleRequestClose();
    }
  }

  handleRequestClose = () => {
    this.setState({
      isPopoverOpened: false,
      pattern: ''
    });
    // TODO uncomment when API is done
    // if (this.props.loadItemsForFilter) {
    //   this.props.loadItemsForFilter();
    // }
  }

  handleRequestOpen = () => {
    this.setState({
      isPopoverOpened: true,
      anchorEl: this.refs.target
    });
  }

  renderItems(items) {
    return items.map((item, index) => {
      return (<FilterSelectMenuItem
        key={index}
        item={item}
        onChange={this.handleItemChange}
        checked={this.valueIsSelected[item.id]}
      />);
    });
  }

  renderDivider() {
    return this.props.selectedValues.size && this.getUnselectedItems().length ||
           (this.state.pattern && this.props.selectedValues.size)
      ? <div style={[styles.divider]}/>
      : null;
  }

  renderSearch() {
    if (this.props.noSearch) {
      return null;
    }
    return this.getUnselectedItems().length || this.state.pattern
      ? (
        <SearchField
          style={styles.searchStyle}
          searchIconTheme={styles.searchIcon}
          inputStyle={styles.searchInput}
          placeholder={this.props.searchPlaceholder}
          value={this.state.pattern}
          onChange={this.handleSearchForItem}
        />
      )
      : null;
  }

  renderSelectedLabel() { // todo: better loc
    const selectedItems = !this.props.selectedValues.size
                            ? `: ${this.props.intl.formatMessage({ id: 'Common.All' })}`
                            : this.getSelectedItems().map(item => item.label).join(', ');
    return !this.props.preventSelectedLabel
      ? (
        <EllipsedText className='filter-select-label' style={styles.infoLabel} text={selectedItems}/>
      )
      : null;
  }

  renderItemList(selectedToTop) {
    const { items } = this.props;
    return (<div style={styles.popoverContent}>
      {selectedToTop ? this.renderItems(this.getSelectedItems()) : null}
      {this.renderDivider()}
      {this.renderSearch()}
      {this.renderItems(selectedToTop ? this.getUnselectedItems() : items)}
    </div>);
  }

  render() {
    if (!this.props.items.length) {
      return null;
    }
    const { label, name } = this.props;
    return (
      <div
        ref='target'
        data-qa={name + '-filter'}
        onClick={this.handleRequestOpen}
        style={[styles.base]}
        className='filter-select-menu'>
        <span>{this.props.preventSelectedLabel || !this.props.selectedValues.size ? label : ''}</span>
        {this.renderSelectedLabel()}
        <FontIcon type='ArrowDownSmall' theme={styles.arrow}/>
        <Popover
          style={styles.popover}
          useLayerForClickAway
          canAutoPosition
          animated={false}
          open={this.state.isPopoverOpened}
          anchorEl={this.state.anchorEl}
          anchorOrigin={this.state.anchorOriginType}
          targetOrigin={this.state.targetOriginType}
          onRequestClose={this.handleRequestClose}
        >
          {this.renderItemList(this.props.selectedToTop)}
        </Popover>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer'
  },
  popoverContent: {
    display: 'flex',
    flexWrap: 'wrap',
    flexDirection: 'column',
    minWidth: 130
  },
  divider: {
    borderTop: '1px solid #ccc',
    display: 'flex'
  },
  arrow: {
    Container: {
      position: 'relative',
      top: 1
    }
  },
  infoLabel: {
    maxWidth: 130
  },
  searchInput: {
    padding: '4px 10px'
  },
  searchStyle: {
    padding: 5
  },
  searchIcon: {
    Container: {
      position: 'absolute',
      right: 6,
      top: 6
    },
    Icon: {
      width: 22,
      height: 22
    }
  }
};

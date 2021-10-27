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
import { Fragment, PureComponent } from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';
import Art from '@app/components/Art';
import EllipsedText from 'components/EllipsedText';
import deepEqual from 'deep-equal';

import FontIcon from 'components/Icon/FontIcon';
import Checkbox from 'components/Fields/Checkbox';
import { SearchField } from 'components/Fields';
import { SelectView } from './SelectView';
import './FilterSelectMenu.less';

FilterSelectMenuItem.propTypes = {
  item: PropTypes.object.isRequired,
  onChange: PropTypes.func.isRequired,
  checked: PropTypes.bool,
  name: PropTypes.string,
  itemIndex: PropTypes.number,
  onClick: PropTypes.func,
  className: PropTypes.string,
  checkBoxClass: PropTypes.string,
  showCheckIcon: PropTypes.bool,
  disabled: PropTypes.bool
};


export function FilterSelectMenuItem({
  item,
  itemIndex,
  onChange,
  checked,
  name,
  onClick,
  className,
  checkBoxClass,
  showCheckIcon,
  disabled
}) {
  const menuClass = clsx('filterSelectMenu', className);
  return (
    <div className={menuClass} >
      <span className='filterSelectMenu__checkbox' key={item.id}>
        <Checkbox
          onChange={() => onChange(checked, item.id)}
          label={[
            item.icon && <FontIcon type={item.icon} key={'fi_' + item.id} theme={{ Container: styles.checkboxLabelContainer }} />,
            item.label
          ]}
          checked={checked}
          dataQa={getDataQaForFilterItem(item.id)}
          checkBoxClass={checkBoxClass}
          showCheckIcon={showCheckIcon}
          disabled={disabled}
        />
      </span>
      <span className='filterSelectMenu__vectorColumns'>
        {
          name === 'col' && <>
            <Art src='UpVector.svg' alt='icon' title='icon'
              className='filterSelectMenu__upVector'
              onClick={() => onClick('up', itemIndex)}
              data-qa='UpVector'
            />
            <Art src='DownVector.svg' alt='icon' title='icon'
              className='filterSelectMenu__downVector'
              onClick={() => onClick('down', itemIndex)}
              data-qa='DownVector'
            />
          </>
        }

      </span>
    </div>
  );
}

@injectIntl
export default class FilterSelectMenu extends PureComponent {

  static propTypes = {
    items: PropTypes.array, // [{label: string, id: string, icon: component}]
    hideOnSelect: PropTypes.bool,
    selectedValues: PropTypes.instanceOf(Immutable.List),
    selectClass: PropTypes.string,
    name: PropTypes.string,
    label: PropTypes.string,
    searchPlaceholder: PropTypes.string,
    className: PropTypes.string,
    checkBoxClass: PropTypes.string,
    iconClass: PropTypes.string,
    showCheckIcon: PropTypes.bool,

    alwaysShowLabel: PropTypes.bool,
    preventSelectedLabel: PropTypes.bool,
    noSearch: PropTypes.bool,
    selectedToTop: PropTypes.bool,
    isArtIcon: PropTypes.bool,

    // callbacks
    loadItemsForFilter: PropTypes.func,
    onItemSelect: PropTypes.func,
    onItemUnselect: PropTypes.func,

    intl: PropTypes.object.isRequired,
    showSelectedLabel: PropTypes.bool,
    icon: PropTypes.string,
    iconStyle: PropTypes.object,
    onClick: PropTypes.func,
    popoverFilters: PropTypes.string,
    selectViewBeforeOpen: PropTypes.func,
    selectViewBeforeClose: PropTypes.func,
    setBackGroundColorForLabel: PropTypes.bool,
    ellipsedTextClass: PropTypes.string
  };

  static defaultProps = { // todo: `la` loc not building correctly here
    items: [],
    selectedValues: Immutable.List(),
    searchPlaceholder: ('Search'),
    showSelectedLabel: true
  };

  constructor(props) {
    super(props);
    this.updateValueIsSelected(props);
  }

  state = {
    pattern: ''
  };

  shouldComponentUpdate(nextProps) {
    const { items, selectedValues } = this.props;
    if (deepEqual(items, nextProps.items) && deepEqual(selectedValues.toJS(), nextProps.selectedValues.toJS())) {
      return false;
    } else {
      return true;
    }
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
  };

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
  };

  beforeDDClose = () => {
    const { selectViewBeforeClose } = this.props;
    if (selectViewBeforeClose && typeof selectViewBeforeClose === 'function') {
      selectViewBeforeClose();
    }
    this.setState({
      pattern: ''
    });
  };
  beforeDDOpen = () => {
    const { selectViewBeforeOpen } = this.props;
    if (selectViewBeforeOpen && typeof selectViewBeforeOpen === 'function') {
      selectViewBeforeOpen();
    }
  }
  renderItems(items) {
    return items.map((item, index) => {
      return (<FilterSelectMenuItem
        key={index}
        item={item}
        onChange={this.handleItemChange}
        checked={this.valueIsSelected[item.id]}
        name={this.props.name}
        itemIndex={index}
        onClick={this.props.onClick}
        className={this.props.className}
        checkBoxClass={this.props.checkBoxClass}
        showCheckIcon={this.props.showCheckIcon}
        disabled={item.disabled}
      />);
    });
  }

  renderDivider() {
    const { selectedValues } = this.props;
    const { pattern } = this.state;
    return ((selectedValues.size && this.getUnselectedItems().length) ||
      (pattern && selectedValues.size > 0)) && <div style={styles.divider} />;
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
    const { ellipsedTextClass } = this.props;
    const selectedItems = !this.props.selectedValues.size
      ? `: ${this.props.intl.formatMessage({ id: 'Common.All' })}`
      : (this.props.alwaysShowLabel ? ': ' : '') + this.getSelectedItems().map(item => item.label).join(', ');
    return !this.props.preventSelectedLabel &&
      <EllipsedText
        className={clsx('filter-select-label', ellipsedTextClass)}
        style={styles.infoLabel}
        text={selectedItems}
      />;
  }

  renderItemList(selectedToTop) {
    const { items } = this.props;
    const currentUnselectedItems = this.getUnselectedItems();
    return (<div style={styles.popoverContent}>
      {this.renderSearch()}
      {selectedToTop ? this.renderItems(this.getSelectedItems()) : null}
      {currentUnselectedItems.length > 0 ? this.renderDivider() : null}
      {this.renderItems(selectedToTop ? currentUnselectedItems : items)}
    </div>);
  }

  render() {
    const {
      label,
      name,
      showSelectedLabel,
      icon,
      iconStyle,
      popoverFilters,
      selectClass,
      isArtIcon,
      iconClass,
      items,
      selectedValues,
      preventSelectedLabel,
      alwaysShowLabel
    } = this.props;

    if (!items.length) {
      return null;
    }
    const className = clsx('filter-select-menu field', selectClass);
    return (
      <SelectView
        content={
          <Fragment>
            {(preventSelectedLabel || !selectedValues.size || alwaysShowLabel) && <span>{label}</span>}
            {showSelectedLabel ? this.renderSelectedLabel() : label}
          </Fragment>
        }
        beforeClose={this.beforeDDClose}
        beforeOpen={this.beforeDDOpen}
        className={className}
        dataQa={name + '-filter'}
        icon={icon}
        iconStyle={iconStyle}
        popoverFilters={popoverFilters}
        isArtIcon={isArtIcon}
        iconClass={iconClass}
      >
        {this.renderItemList(this.props.selectedToTop)}
      </SelectView>
    );
  }
}

/**
 * Generates data-qa attribute for filters.
 *
 * E2e test are build in assumption that FilterSelectMenu and {@see JobsFilters} have the same shape
 * of data-qa attributes. This method is created to explicitly define that relation
 * @param {string|number} id - filter id
 */
export const getDataQaForFilterItem = (id) => id + '-filter-item';

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
    minWidth: 240,
    paddingLeft: 16,
    paddingRight: 16,
    paddingTop: 8,
    paddingBottom: 8
  },
  divider: {
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
  searchIcon: {
    Container: {
      position: 'absolute',
      right: 6,
      top: 3
    },
    Icon: {
      width: 22,
      height: 22,
      marginTop: '3px'
    }
  },
  checkboxLabelContainer: {
    overflow: 'hidden',
    height: 24,
    width: 24
  }
};

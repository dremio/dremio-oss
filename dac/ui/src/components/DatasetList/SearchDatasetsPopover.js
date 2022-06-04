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
import { createRef, PureComponent } from 'react';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import Immutable  from 'immutable';
import Radium from 'radium';
import {injectIntl} from 'react-intl';

import { loadSearchData } from 'actions/search.js';
import { getSearchResult } from 'selectors/resources';
import { Popover } from '@app/components/Popover';
import SearchDatasetsComponent from '@inject/components/DatasetList/SearchDatasetsComponent';

import { compose } from 'redux';
import DatasetList from './DatasetList';

import './SearchDatasetsPopover.less';

const DELAY_SEARCH = 1000;
@Radium
class SearchDatasetsPopover extends PureComponent {

  static propTypes = {
    searchData: PropTypes.instanceOf(Immutable.List).isRequired,
    loadSearchData: PropTypes.func.isRequired,
    changeSelectedNode: PropTypes.func.isRequired,
    dragType: PropTypes.string,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired
  }

  constructor(props) {
    super(props);

    this.state = {
      filter: '',
      searchVisible: false,
      anchorEl: null,
      closeVisible: false
    };

    this.searchField = createRef();
  }

  componentWillUnmount() {
    clearTimeout(this.updateSearch);
  }

  componentDidUpdate(newProps) {
    this.propsChange(this.props, newProps);
  }

  propsChange(prevProps, newProps) {
    const {
      searchText
    } = newProps;

    if (prevProps.searchText !== searchText && typeof searchText === 'string') {
      this.input.value = searchText;
      this.input.focus();
      this.onInput(); // simulate text change
    }

    if (this.state.filter === '') {
      this.setState({ closeVisible: false });
    } else {
      this.setState({ closeVisible: true });
    }
  }

  handleSearchShow() {
    this.setState({ searchVisible: true });
  }

  handleSearchHide = () => {
    this.setState({ searchVisible: false, closeVisible: false });
    this.input.focus();
  }

  startSearch(text) {
    this.props.loadSearchData(text);
    this.handleSearchShow();
  }

  onInput = () => {
    const text = this.input.value;

    this.setState({
      anchorEl: this.input,
      filter: text
    });

    clearTimeout(this.updateSearch);

    this.updateSearch = setTimeout(() => {
      this.startSearch(text);
    }, DELAY_SEARCH);
  }

  onInputRef = input => {
    this.input = input;
  }

  clearFilter = () => {
    this.input.value = '';
    this.handleSearchHide();
    this.setState({ filter: '' });
  }

  render() {
    const value = this.state.filter;
    const { searchVisible, anchorEl } = this.state;
    const { dragType, searchData, changeSelectedNode } = this.props;
    return  (
      <div className='resource-tree' style={[styles.base, this.props.style]}>
        <SearchDatasetsComponent
          onInput={this.onInput}
          clearFilter={this.clearFilter}
          closeVisible={this.state.closeVisible}
          onInputRef={this.onInputRef}
        />

        <Popover
          anchorEl={searchVisible ? anchorEl : null}
          style={styles.searchStyle}
          onClose={this.handleSearchHide}
        >
          <DatasetList
            dragType={dragType}
            data={searchData}
            changeSelectedNode={changeSelectedNode}
            addtoEditor={this.props.addtoEditor}
            inputValue={value}
            isInProgress={false}
            showParents
            shouldAllowAdd
          />
        </Popover>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    position: 'relative'
  },
  location: {
    margin: '7px 0 0'
  },
  searchStyle: {
    margin: '9px 0 0 -18px',
    zIndex: 1001
  },
  fontIcon: {
    'Icon': {
      width: 24,
      height: 24
    },
    'Container': {
      width: 24,
      height: 24
    }
  },
  clearIcon: {
    Icon: {
      width: 22,
      height: 22
    },
    Container: {
      cursor: 'pointer',
      width: 22,
      height: 22
    }
  }
};

const mapStateToProps = (state) => ({
  searchData: getSearchResult(state) || Immutable.List()
});

export default compose(connect(mapStateToProps, { loadSearchData }), injectIntl)(SearchDatasetsPopover);

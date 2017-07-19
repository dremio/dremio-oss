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
import { Component, PropTypes } from 'react';
import { connect }   from 'react-redux';
import Radium from 'radium';
import Immutable from 'immutable';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';

import FontIcon from 'components/Icon/FontIcon';
import DatasetsSearch from 'components/DatasetsSearch';
import {loadSearchData} from 'actions/search';
import { getSearchResult, getViewState } from 'selectors/resources';
import { bodyWhite } from 'uiTheme/radium/typography';

@Radium
export class SearchItem extends Component {
  static propTypes = {
    loadSearchData: PropTypes.func,
    search: PropTypes.instanceOf(Immutable.List).isRequired,
    searchViewState: PropTypes.instanceOf(Immutable.Map)
  }

  constructor(props) {
    super(props);
    this.state = {
      searchVisible: false,
      inputText: '',
      anchorEl: null
    };
  }

  componentWillUnmount() {
    clearTimeout(this.updateSearch);
  }

  onInput = (evt) => {
    this.setState({
      anchorEl: evt.currentTarget
    });
    const text = this.refs.input.value;
    clearTimeout(this.updateSearch);
    this.updateSearch = setTimeout(() => (this.startSearch(text)), 800);
  }

  getInputText() {
    return (
      <div style={styles.searchItem} className='search-item'>
        <FontIcon
          type='SearchPaleNavy'
          theme={styles.fontIcon}/>
        <input
          type='text'
          ref='input'
          onInput={this.onInput}
          style={{...styles.searchInput, outline: 'none'}}/>
      </div>
    );
  }

  handleSearchShow() {
    this.setState({searchVisible: true});
  }

  handleSearchHide = () => {
    this.setState({searchVisible: false});
    this.refs.input.value = '';
  }

  startSearch(text) {
    this.props.loadSearchData(text);
    this.setState({inputText: text});
    this.handleSearchShow();
  }

  render() {
    const {searchVisible, inputText} = this.state;
    const {search, searchViewState} = this.props;
    return (
      <div style={[styles.table]}>
        <div style={[styles.row]}>
          <div style={[styles.col1]}>{this.getInputText()}</div>
        </div>
        <Popover
          anchorEl={this.state.anchorEl}
          open={searchVisible}
          style={styles.searchStyle}
          onRequestClose={this.handleSearchHide}
          animation={PopoverAnimationVertical}>
          <DatasetsSearch
            globalSearch
            searchData={search}
            searchViewState={searchViewState}
            visible={searchVisible}
            inputValue={inputText}
            handleSearchHide={this.handleSearchHide}/>
        </Popover>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    search: getSearchResult(state) || Immutable.List(),
    searchViewState: getViewState(state, 'searchDatasets')
  };
}

export default connect(mapStateToProps, {loadSearchData})(SearchItem);

const styles = {
  searchStyle: {
    margin: '9px 0 0 -18px',
    right: 50 // not quite the to comp (0) - but avoids a Popover layout bug
  },
  searchItem: {
    display: 'flex'
  },
  searchInput: {
    ...bodyWhite,
    backgroundColor: 'transparent',
    border: 0,
    height: 20,
    transition: 'all .3s',
    ':focus': {
      width: 240
    }
  },
  table: {
    display: 'flex',
    alignItems: 'center',
    height: 55,
    margin: '0 0 0 12px'
  },
  row: {
    borderBottom: '1px solid rgba(255, 255, 255, .5)',
    width: 140,
    transition: 'width 0.6s',
    ':focus': {
      width: 260
    }
  },
  col1: {
    display: 'table-cell',
    verticalAlign: 'middle'
  },
  fontIcon: {
    'Icon': {
      width: 22,
      height: 22
    },
    'Container': {
      marginLeft: -4,
      width: 22,
      height: 22,
      marginTop: -3
    }
  }
};

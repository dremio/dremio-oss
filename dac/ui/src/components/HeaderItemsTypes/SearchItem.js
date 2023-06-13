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
import { Component } from "react";
import { compose } from "redux";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { Popover } from "@app/components/Popover";
import { injectIntl } from "react-intl";

import DatasetsSearch from "components/DatasetsSearch";
import { loadSearchData } from "actions/search";
import { getSearchResult, getViewState } from "selectors/resources";
import { getSearchText } from "@app/selectors/search";

import * as classes from "./SearchItem.less";

export class SearchItem extends Component {
  static propTypes = {
    loadSearchData: PropTypes.func,
    search: PropTypes.instanceOf(Immutable.List).isRequired,
    searchViewState: PropTypes.instanceOf(Immutable.Map),
    searchText: PropTypes.string,
    intl: PropTypes.object.isRequired,
  };
  input = null; // ill store input ref

  constructor(props) {
    super(props);
    this.state = {
      searchVisible: false,
      inputText: "",
      anchorEl: null,
    };
  }

  componentWillUnmount() {
    clearTimeout(this.updateSearch);
  }

  componentWillUpdate(newProps) {
    this.propsChange(this.props, newProps);
  }

  propsChange(prevProps, newProps) {
    const { searchText } = newProps;

    if (prevProps.searchText !== searchText && typeof searchText === "string") {
      this.input.value = searchText;
      this.input.focus();
      this.onInput(); // simulate text change
    }
  }

  onInput = () => {
    this.setState({
      anchorEl: this.input,
    });
    const text = this.input.value;
    clearTimeout(this.updateSearch);
    this.updateSearch = setTimeout(() => this.startSearch(text), 800);
  };

  onFocus = () => {
    this.searchTableRowRef.className = "searchTableRow --focused";
  };

  onBlur = () => {
    this.searchTableRowRef.className = "searchTableRow";
  };

  getInputText() {
    const { intl } = this.props;

    const placeholderText = intl.formatMessage({
      id: "SideNav.SearchPlaceHolder",
    }); //('Search Catalog...');
    return (
      <div className="searchItem search-item">
        <dremio-icon
          name="interface/search"
          class={classes["searchIcon"]}
        ></dremio-icon>
        <input
          key="textInput"
          type="text"
          placeholder={placeholderText}
          ref={this.onInputRef}
          onInput={this.onInput}
          className={"searchInput"}
          onFocus={this.onFocus}
          onBlur={this.onBlur}
        />
      </div>
    );
  }

  handleSearchShow() {
    this.setState({ searchVisible: true });
  }

  handleSearchHide = () => {
    this.setState({ searchVisible: false });
  };

  startSearch(text) {
    this.props.loadSearchData(text);
    this.setState({ inputText: text });
    this.handleSearchShow();
  }

  onInputRef = (input) => {
    this.input = input;
  };

  onFocusRef = (div) => {
    this.searchTableRowRef = div;
  };

  render() {
    const { searchVisible, inputText, anchorEl } = this.state;
    const { search, searchViewState } = this.props;

    const popoverStyle = styles.searchStyle;

    return (
      <div className={"searchTable"}>
        <div className={"searchTableRow"} ref={this.onFocusRef}>
          <div className={"searchTableCol1"}>{this.getInputText()}</div>
        </div>
        <Popover
          anchorEl={searchVisible ? anchorEl : null}
          style={popoverStyle}
          onClose={this.handleSearchHide}
        >
          <DatasetsSearch
            globalSearch
            searchData={search}
            searchViewState={searchViewState}
            inputValue={inputText}
            handleSearchHide={this.handleSearchHide}
          />
        </Popover>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    search: getSearchResult(state) || Immutable.List(),
    searchViewState: getViewState(state, "searchDatasets"),
    searchText: getSearchText(state),
  };
}

export default compose(
  connect(mapStateToProps, { loadSearchData }),
  injectIntl
)(SearchItem);

const styles = {
  searchStyle: {
    margin: "9px 0 0 -18px",
    width: 650,
    zIndex: 1001,
  },
};

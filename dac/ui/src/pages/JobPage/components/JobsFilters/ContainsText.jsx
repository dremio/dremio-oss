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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";
import { injectIntl } from "react-intl";
import "./ContainsText.less";

class ContainsText extends PureComponent {
  static propTypes = {
    onEnterText: PropTypes.func.isRequired,
    defaultValue: PropTypes.string,
    placeholderId: PropTypes.string,
    intl: PropTypes.object.isRequired,
    className: PropTypes.string,
    searchIconClass: PropTypes.string,
  };

  constructor(props) {
    super(props);
    this.state = {
      searchInput: "",
    };
  }

  getSearchText() {
    return this.state.searchInput;
  }

  handleContainsEnterText(e) {
    const { onEnterText } = this.props;
    const text = e.target.value;
    this.setState(
      {
        searchInput: text,
      },
      onEnterText.bind(this, text),
    );
  }

  onFocusRef = (div) => {
    this.containsTextRef = div;
  };

  onFocus = () => {
    const { className } = this.props;

    this.containsTextRef.className = classNames(
      "containsText",
      className,
      "--focused",
    );
  };

  onBlur = () => {
    const { className } = this.props;

    this.containsTextRef.className = classNames("containsText", className);
  };

  render() {
    const {
      defaultValue,
      intl,
      className,
      searchIconClass,
      placeholderId = "Job.SearchJobs",
    } = this.props;
    const placeholderText = intl.formatMessage({ id: placeholderId });

    return (
      <div
        className={classNames("containsText", className)}
        ref={this.onFocusRef}
      >
        <dremio-icon
          name="interface/search"
          alt="search"
          class={classNames(
            "containsText__searchIcon",
            searchIconClass,
            "icon-primary",
          )}
        />
        <input
          className="form-placeholder"
          defaultValue={defaultValue}
          type="text"
          placeholder={placeholderText}
          style={styles.searchInput}
          onInput={this.handleContainsEnterText.bind(this)}
          onFocus={this.onFocus}
          onBlur={this.onBlur}
        />
      </div>
    );
  }
}

const styles = {
  searchInput: {
    display: "block",
    padding: "4px",
    border: "none",
    borderRadius: "4px",
    fontSize: 14,
    width: "300px",
    fontWeight: "normal",
    outline: "none",
    backgroundColor: "var(--fill--primary)",
  },
};
export default injectIntl(ContainsText);

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
import { injectIntl } from "react-intl";
import PropTypes from "prop-types";

import FontIcon from "../Icon/FontIcon";

const SearchDatasetsComponent = (props) => {
  const {
    placeholderText,
    intl,
    onInputRef,
    onInput,
    clearFilter,
    dataQa,
    closeVisible,
  } = props;
  const placeholder = placeholderText
    ? placeholderText
    : intl.formatMessage({ id: "Explore.SearchDatasets" });

  return (
    <div className="searchDatasetsPopover">
      <FontIcon key="icon" type="Search" theme={styles.fontIcon} />
      <input
        key="textInput"
        type="text"
        placeholder={placeholder}
        ref={onInputRef}
        onInput={onInput}
        className={"searchInput"}
        data-qa={dataQa}
      />
      {closeVisible && (
        <FontIcon type="XBig" theme={styles.clearIcon} onClick={clearFilter} />
      )}
    </div>
  );
};

SearchDatasetsComponent.propTypes = {
  onInput: PropTypes.func,
  clearFilter: PropTypes.func,
  closeVisible: PropTypes.bool,
  onInputRef: PropTypes.any,
  intl: PropTypes.object.isRequired,
  placeholderText: PropTypes.string,
  dataQa: PropTypes.string,
};

const styles = {
  fontIcon: {
    Icon: {
      width: 24,
      height: 24,
    },
    Container: {
      width: 24,
      height: 24,
    },
  },
  clearIcon: {
    Icon: {
      width: 22,
      height: 22,
    },
    Container: {
      cursor: "pointer",
      width: 22,
      height: 22,
    },
  },
};

export default injectIntl(SearchDatasetsComponent);

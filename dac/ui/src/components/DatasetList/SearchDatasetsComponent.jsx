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
import { IconButton } from "dremio-ui-lib/components";

const SearchDatasetsComponent = (props) => {
  const {
    placeholderText,
    intl,
    onInputRef,
    onInput,
    clearFilter,
    dataQa,
    closeVisible,
    onFocus,
    onBlur,
    onFocusRef,
  } = props;
  const placeholder = placeholderText
    ? placeholderText
    : intl.formatMessage({ id: "Explore.SearchDatasets" });

  return (
    <div className="searchDatasetsPopover" ref={onFocusRef}>
      <dremio-icon name="interface/search" class="icon-primary"></dremio-icon>
      <input
        key="textInput"
        type="text"
        placeholder={placeholder}
        ref={onInputRef}
        onInput={onInput}
        className={"searchInput"}
        data-qa={dataQa}
        onFocus={onFocus}
        onBlur={onBlur}
      />
      {closeVisible && (
        <IconButton onClick={clearFilter} aria-label="Clear">
          <dremio-icon
            name="interface/close-small"
            style={{
              cursor: "pointer",
            }}
          ></dremio-icon>
        </IconButton>
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
  onFocus: PropTypes.func,
  onFocusRef: PropTypes.any,
  onBlur: PropTypes.func,
};

export default injectIntl(SearchDatasetsComponent);

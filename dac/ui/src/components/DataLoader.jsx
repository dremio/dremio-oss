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
import { PureComponent, Fragment } from "react";
import PropTypes from "prop-types";
import { KeyChangeTrigger } from "@app/components/KeyChangeTrigger";

/**
 * A utility class, that monitors {@see #keyValue} property change and triggers {#onChange} if key value
 * was changed. Also it triggers onChange on component mount.
 */
export class DataLoader extends PureComponent {
  static propTypes = {
    keyValue: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.bool,
      PropTypes.number,
    ]),
    isInvalidated: PropTypes.bool,
    onChange: PropTypes.func.isRequired,
  };

  onIsInvalidatedChange = (isInvalidated) => {
    if (isInvalidated) {
      this.props.onChange();
    }
  };

  render() {
    const { keyValue, isInvalidated, onChange } = this.props;

    return (
      <Fragment>
        <KeyChangeTrigger keyValue={keyValue} onChange={onChange} />
        <KeyChangeTrigger
          keyValue={isInvalidated}
          onChange={this.onIsInvalidatedChange}
          callOnMount={false} // first KeyChangeTrigger will call onChange on mount
        />
      </Fragment>
    );
  }
}

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
import shallowEqual from "shallowequal";

/**
 * A utility class, that monitors {@see #keyValue} property change and triggers {#onChange} if key value
 * was changed. Also it triggers onChange on component mount.
 */
export class KeyChangeTrigger extends PureComponent {
  static propTypes = {
    keyValue: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.bool,
      PropTypes.number,
      PropTypes.object,
      PropTypes.array,
    ]),
    onChange: PropTypes.func.isRequired,
    callOnMount: PropTypes.bool,
    children: PropTypes.any,
  };

  static defaultProps = {
    callOnMount: true,
  };

  onChange() {
    this.props.onChange(this.props.keyValue);
  }

  componentDidMount() {
    if (this.props.callOnMount) {
      this.onChange();
    }
  }

  componentDidUpdate(prevProps) {
    if (this.isKeyValueChanged(prevProps.keyValue, this.props.keyValue)) {
      this.onChange();
    }
  }

  isKeyValueChanged(oldValue, newValue) {
    if (typeof newValue === "object") {
      // if array or object use shallow equal
      return !shallowEqual(oldValue, newValue);
    }
    return oldValue !== newValue;
  }

  render() {
    return this.props.children || null;
  }
}

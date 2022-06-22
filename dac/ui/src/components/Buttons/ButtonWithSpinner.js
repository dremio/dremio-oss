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
import PropTypes from "prop-types";

import { Button } from "dremio-ui-lib";
import FontIcon from "components/Icon/FontIcon";

import * as classes from "./buttonWithSpinner.less";

const ButtonWithSpinner = (props) => {
  const {
    type,
    color,
    variant,
    inProgress,
    text,
    onClick,
    className,
    buttonClassName,
    disableMargin,
    children,
    ...otherProps
  } = props;

  const renderSpinner = () => (
    <FontIcon type="Loader spinner" theme={styles.spinner} />
  );

  return (
    <div className={`${classes.buttonWithSpinner} ${className}`}>
      <Button
        color={color}
        variant={variant}
        text={text}
        type={type}
        disabled={inProgress}
        onClick={onClick}
        className={buttonClassName}
        disableMargin={disableMargin}
        {...otherProps}
      >
        {inProgress ? renderSpinner() : children}
      </Button>
    </div>
  );
};

ButtonWithSpinner.propTypes = {
  className: PropTypes.string,
  buttonClassName: PropTypes.string,
  color: PropTypes.string,
  variant: PropTypes.string,
  type: PropTypes.string,
  disableMargin: PropTypes.bool,
  inProgress: PropTypes.bool,
  text: PropTypes.string,
  onClick: PropTypes.func,
  children: PropTypes.any,
};

ButtonWithSpinner.defaultProps = {
  variant: "contained",
  className: "",
  buttonClassName: "",
  inProgress: false,
};

const styles = {
  spinner: {
    Container: {
      display: "block",
      height: 24,
    },
  },
};

export default ButtonWithSpinner;

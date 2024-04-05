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
import React, { useEffect, useState } from "react";
import { styled } from "@mui/material/styles";
import Proptypes from "prop-types";

import clsx from "clsx";

import { ReactComponent as Expand } from "../../art/ArrowRight.svg";
import { ReactComponent as Collapse } from "../../art/ArrowDown.svg";

import "./expandableText.scss";

const PREFIX = "ExpandableText";

const classes = {
  label: `${PREFIX}-label`,
};

const Root = styled("div")(({ theme }) => {
  const { palette: { primary: { main } = {} } = {} } = theme || {};

  return {
    [`& .${classes.label}`]: {
      color: main,
    },
  };
});
Root.displayName = "ExpandableTextRoot";

const ExpandableText = (props) => {
  const {
    classes = {},
    children,
    defaultExpanded,
    hideOnlyOnIcon,
    open,
    label,
    indentChildren,
    onClick,
    onToggle,
    text,
  } = props;

  const [expanded, setExpanded] = useState(defaultExpanded);

  useEffect(() => {
    if (open !== undefined) {
      setExpanded(open);
    }
  }, [open]);

  const handleIconClick = (event) => {
    if (hideOnlyOnIcon && expanded) {
      setExpanded(!expanded);
      if (onToggle && typeof onToggle === "function") {
        onToggle(!expanded);
      }
      event.stopPropagation();
    }
  };

  const handleLabelClick = () => {
    let updatedIsExpanded = expanded;
    if (!hideOnlyOnIcon || !expanded) {
      updatedIsExpanded = !expanded;
      setExpanded(updatedIsExpanded);
      if (onToggle && typeof onToggle === "function") {
        onToggle(updatedIsExpanded);
      }
    }
    if (onClick && typeof onClick === "function") {
      onClick(updatedIsExpanded);
    }
  };

  const rootClasses = clsx("expandable-text-root", {
    [classes.root]: classes.root,
  });
  const labelContainerClasses = clsx(
    "expandable-text-label-container",
    "noselect",
    { [classes.labelContainer]: classes.labelContainer },
  );
  const labelClasses = clsx("expandable-text-label", classes.label, {
    [classes.label]: classes.label,
  });
  const collapsableContainerClasses = clsx(
    "collapsable-container",
    { "indented-collapsable-container": indentChildren },
    { [classes.collapsableContainer]: classes.collapsableContainer },
  );

  const Icon = expanded ? Collapse : Expand;
  return (
    <Root className={rootClasses}>
      <div className={labelContainerClasses} onClick={handleLabelClick}>
        <div className="expandable-text-label-icon" onClick={handleIconClick}>
          <Icon fontSize="small" />
        </div>
        <span className={labelClasses} title={text}>
          {label}
        </span>
      </div>
      {expanded && (
        <div className={collapsableContainerClasses}>{children}</div>
      )}
    </Root>
  );
};

ExpandableText.propTypes = {
  classes: Proptypes.shape({
    root: Proptypes.string,
    labelContainer: Proptypes.string,
    label: Proptypes.string,
    collapsableContainer: Proptypes.string,
  }),
  children: Proptypes.oneOfType([
    Proptypes.string,
    Proptypes.node,
    Proptypes.arrayOf(Proptypes.node),
  ]).isRequired,
  defaultExpanded: Proptypes.bool,
  hideOnlyOnIcon: Proptypes.bool,
  indentChildren: Proptypes.bool,
  onClick: Proptypes.func,
  onToggle: Proptypes.func,
  label: Proptypes.oneOfType([
    Proptypes.string,
    Proptypes.node,
    Proptypes.arrayOf(Proptypes.node),
  ]).isRequired,
  open: Proptypes.oneOfType([Proptypes.bool, Proptypes.object]),
  text: Proptypes.string,
};

ExpandableText.defaultProps = {
  classes: {},
  defaultExpanded: false,
  hideOnlyOnIcon: false,
  indentChildren: false,
};

export default ExpandableText;

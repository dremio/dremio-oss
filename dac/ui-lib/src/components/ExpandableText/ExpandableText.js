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
import Proptypes from "prop-types";

import clsx from "clsx";

import { makeStyles } from "@material-ui/core/styles";

import { ReactComponent as Expand } from "../../art/ArrowRight.svg";
import { ReactComponent as Collapse } from "../../art/ArrowDown.svg";

import "./expandableText.scss";

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
  } = props;

  const [expanded, setExpanded] = useState(defaultExpanded);

  useEffect(() => {
    if (open !== undefined) {
      setExpanded(open);
    }
  }, [open]);

  const useStylesBase = makeStyles((theme) => {
    const { palette: { primary: { main } = {} } = {} } = theme || {};

    return {
      label: {
        color: main,
      },
    };
  });

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

  const classesBase = useStylesBase();

  const rootClasses = clsx("expandable-text-root", {
    [classes.root]: classes.root,
  });
  const labelContainerClasses = clsx(
    "expandable-text-label-container",
    "noselect",
    { [classes.labelContainer]: classes.labelContainer }
  );
  const labelClasses = clsx("expandable-text-label", classesBase.label, {
    [classes.label]: classes.label,
  });
  const collapsableContainerClasses = clsx(
    "collapsable-container",
    { "indented-collapsable-container": indentChildren },
    { [classes.collapsableContainer]: classes.collapsableContainer }
  );

  const Icon = expanded ? Collapse : Expand;
  return (
    <div className={rootClasses}>
      <div className={labelContainerClasses} onClick={handleLabelClick}>
        <div className="expandable-text-label-icon" onClick={handleIconClick}>
          <Icon fontSize="small" />
        </div>
        <div className={labelClasses}>{label}</div>
      </div>
      {expanded && (
        <div className={collapsableContainerClasses}>{children}</div>
      )}
    </div>
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
};

ExpandableText.defaultProps = {
  classes: {},
  defaultExpanded: false,
  hideOnlyOnIcon: false,
  indentChildren: false,
};

export default ExpandableText;

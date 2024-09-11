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

import * as React from "react";
import clsx from "clsx";
import { Tooltip } from "./Tooltip/Tooltip";
import { useHasClipboardPermissions } from "./utilities/useHasClipboardPermissions";
import { type Placement } from "@floating-ui/react";

const writeToClipboard = (text: string): Promise<void> => {
  return navigator.clipboard.writeText(text);
};

type Props = {
  children: JSX.Element;
  contents: string;
  className?: string;
  placement?: Placement;
  copyTooltipLabel?: string;
  portal?: boolean;
};

export const CopyContainer = (props: Props) => {
  const { copyTooltipLabel = "Copy", portal = true } = props;
  const hasPermission = useHasClipboardPermissions();
  const [hasCopied, setHasCopied] = React.useState(false);

  const tooltipText = hasPermission ? (
    hasCopied ? (
      <span>Copied</span>
    ) : (
      <span>{copyTooltipLabel}</span>
    )
  ) : (
    <span>Copy is disabled on insecure connections</span>
  );

  return (
    <Tooltip
      portal={portal}
      placement={props.placement || "top"}
      content={tooltipText}
      onClose={() => {
        setHasCopied(false);
      }}
    >
      <div className={clsx("max-w-max", props.className)}>
        {React.cloneElement(props.children, {
          onClick: (e: any) => {
            e.stopPropagation();
            writeToClipboard(props.contents);
            setHasCopied(true);
          },
          disabled: !hasPermission,
        })}
      </div>
    </Tooltip>
  );
};

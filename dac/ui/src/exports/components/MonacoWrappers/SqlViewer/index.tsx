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

import type { FC, HTMLProps } from "react";
import clsx from "clsx";
import { SqlViewer as Viewer } from "dremio-ui-common/sonar/components/Monaco/components/SqlViewer/SqlViewer.js";
import * as classes from "./SqlViewer.module.less";

// should match the props from ui-common
type SqlViewerProps = HTMLProps<HTMLDivElement> & {
  fitHeightToContent?: boolean;
  value: string;
};

/**
 * OSS wrapper of the read-only version of the SQL editor used to apply styles
 */
export const SqlViewer: FC<SqlViewerProps> = ({ className, ...props }) => {
  return (
    <Viewer {...props} className={clsx(className, classes["sql-viewer"])} />
  );
};

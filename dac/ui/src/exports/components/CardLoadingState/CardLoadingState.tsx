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
import { Spinner } from "dremio-ui-lib/components";
import * as classes from "./CardLoadingState.module.less";
import clsx from "clsx";

const CardLoadingState = ({
  loadingText = "Loading",
  iconName,
}: {
  loadingText?: string;
  iconName?: string;
}) => {
  return (
    <div className={classes["card-loading"]}>
      <Spinner iconName={iconName} />
      <span className={clsx(classes["card-loading-text"], "text-sm")}>
        {loadingText}
      </span>
    </div>
  );
};

export default CardLoadingState;

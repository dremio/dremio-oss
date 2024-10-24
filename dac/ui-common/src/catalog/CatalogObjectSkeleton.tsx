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

import { Skeleton } from "dremio-ui-lib/components";

export const CatalogObjectSkeleton = () => (
  <div className="flex flex-row items-center gap-05 h-full">
    <Skeleton
      type="custom"
      style={{
        width: "20px",
        height: "20px",
        marginLeft: "26px",
        marginRight: "2px",
      }}
    />
    <Skeleton width="15ch" />
  </div>
);

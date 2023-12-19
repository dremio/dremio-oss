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
import { useEffect } from "react";
import { useBehaviorSubject } from "../../../utilities/useBehaviorSubject";
import type { SQLRunnerSession } from "../SqlRunnerSession.type";
import {
  $SqlRunnerSession,
  closeTab,
  newTab,
  selectTab,
  newPopulatedTab,
} from "../resources/SqlRunnerSessionResource";

export const useSqlRunnerSession = () => {
  const [sqlRunnerSession] = useBehaviorSubject<SQLRunnerSession | null>(
    $SqlRunnerSession.$merged as any
  );

  useEffect(() => {
    if ($SqlRunnerSession.resource.getResource().status === "initial") {
      $SqlRunnerSession.resource.fetch();
    }
  }, []);

  return {
    ...sqlRunnerSession,
    closeTab,
    selectTab,
    newTab,
    newPopulatedTab,
  };
};

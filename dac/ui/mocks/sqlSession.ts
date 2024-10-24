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

import { factory, primaryKey } from "@mswjs/data";
import { rest } from "msw";
import { SonarV2Config } from "./_internal/HandlersConfig";

const emptySqlSession = (userId: string) => ({
  currentScriptId: "",
  scriptIds: [],
  userId,
});

export const sqlSessionModel = {
  sqlSession: {
    currentScriptId: String,
    scriptIds: Array,
    userId: primaryKey(String),
  },
};

type SqlSessionDb = ReturnType<typeof factory<typeof sqlSessionModel>>;

const sqlSessionActions = (db: SqlSessionDb) => ({
  getSession: (userId: string) => {
    const session = db.sqlSession.findFirst({
      where: { userId: { equals: userId } },
    });

    if (!session) {
      const emptySession = emptySqlSession(userId);
      db.sqlSession.create(emptySession);
      return emptySession;
    }

    return session;
  },
  openTab: (userId: string, tabId: string) => {
    return db.sqlSession.update({
      where: { userId: { equals: userId } },
      data: {
        currentScriptId: () => tabId,
        scriptIds: (prev) => {
          if (prev.includes(tabId)) {
            return prev;
          }
          return [...prev, tabId];
        },
      },
    });
  },
  deleteTab: (userId: string, tabId: string) => {
    return db.sqlSession.update({
      where: { userId: { equals: userId } },
      data: {
        currentScriptId: () => tabId,
        scriptIds: (prev) => {
          if (!prev.includes(tabId)) {
            return prev;
          }
          return prev.filter((id) => id !== tabId);
        },
      },
    });
  },
});

export const sqlSessionHandlers = (
  config: SonarV2Config & { db: SqlSessionDb },
) => [
  rest.get(config.createSonarV2Url("sql-runner/session"), (req, res, ctx) => {
    const userId = "1234";
    return res(ctx.json(sqlSessionActions(config.db).getSession(userId)));
  }),
  rest.put(
    config.createSonarV2Url("sql-runner/session/tabs/:tabId"),
    (req, res, ctx) => {
      const userId = "1234";
      return res(
        ctx.json(
          sqlSessionActions(config.db).openTab(userId, req.params["tabId"]),
        ),
      );
    },
  ),
  rest.delete(
    config.createSonarV2Url("sql-runner/session/tabs/:tabId"),
    (req, res, ctx) => {
      const userId = "1234";
      sqlSessionActions(config.db).deleteTab(userId, req.params["tabId"]);
      return res(ctx.status(204));
    },
  ),
];

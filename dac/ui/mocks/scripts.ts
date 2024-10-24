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
import { SonarV2Config, UserContext } from "./_internal/HandlersConfig";
import { v4 as uuidv4 } from "uuid";

type NewScript = {
  content: string;
  context: string[];
  description: string;
  name: string;
};

export const scriptsModel = {
  scripts: {
    content: String,
    context: Array,
    createdAt: Number,
    createdBy: Object,
    description: String,
    id: primaryKey(String),
    jobIds: Array,
    jobResultUrls: Array,
    modifiedAt: Number,
    modifiedBy: Object,
    name: String,
    referencesList: Array,
  },
};

type ScriptsDb = ReturnType<typeof factory<typeof scriptsModel>>;

const scriptActions = (config: UserContext & { db: ScriptsDb }) => ({
  list: () => config.db.scripts.getAll(),
  create: (script: NewScript) => {
    const newScript = {
      content: script.content,
      context: script.context,
      createdAt: new Date().getTime(),
      createdBy: config.user,
      description: script.description,
      id: uuidv4(),
      jobIds: [],
      jobResultUrls: [],
      modifiedAt: new Date().getTime(),
      modifiedBy: config.user,
      name: script.name,
      referencesList: [],
    };
    config.db.scripts.create(newScript);
    return newScript;
  },
  update: (id: string, script: NewScript) => {
    return config.db.scripts.update({
      where: { id: { equals: id } },
      data: {
        content: script.content,
        context: script.context,
        description: script.description,
        modifiedAt: new Date().getTime(),
        name: script.name,
      },
    });
  },
  deleteScript: (id: string) => {
    config.db.scripts.delete({ where: { id: { equals: id } } });
  },
});

export const scriptsHandlers = (
  config: SonarV2Config & UserContext & { db: ScriptsDb },
) => [
  rest.get(config.createSonarV2Url("scripts"), (req, res, ctx) => {
    const scripts = scriptActions(config).list();
    return res(
      ctx.json({
        data: scripts,
        total: scripts.length,
      }),
    );
  }),
  rest.post(config.createSonarV2Url("scripts"), async (req, res, ctx) => {
    const body = (await req.json()) as NewScript;
    return res(ctx.json(scriptActions(config).create(body)));
  }),
  rest.put(config.createSonarV2Url("scripts/:id"), async (req, res, ctx) => {
    const id = req.params["id"] as string;
    const body = (await req.json()) as NewScript;
    return res(ctx.json(scriptActions(config).update(id, body)));
  }),
  rest.delete(config.createSonarV2Url("scripts/:id"), (req, res, ctx) => {
    const id = req.params["id"] as string;
    scriptActions(config).deleteScript(id);
    return res(ctx.status(204));
  }),
];

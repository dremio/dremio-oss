/*
 * Copyright (C) 2017 Dremio Corporation
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
//This is a bit hacky, but it won't be necessary after server and client urls match

// full path is the . path used in sql
import { HOME_SPACE_NAME, RECENT_SPACE_NAME } from 'constants/Constants';

export function splitFullPath(fullPath, preserveQuoting) {
  const PATH_DELIMITER = '.';
  const QUOTE = '"';

  const result = [];
  let buffer = '';
  let isInQuote = false;

  function addBufferToResult() {
    // home name will have " around it which we want to remove for paths
    if (!preserveQuoting && buffer.length > 1 && buffer[0] === '"' && buffer[buffer.length - 1] === '"') {
      result.push(buffer.slice(1, -1));
    } else {
      result.push(buffer);
    }

    buffer = '';
  }

  for (let i = 0; i < fullPath.length; i++) {
    const char = fullPath.charAt(i);

    if (char === QUOTE) {
      if (isInQuote) {
        // check for double QUOTE, which means its a escaped value
        if (i < fullPath.length - 1 && fullPath[i + 1] === '"') {
          buffer += QUOTE;
          i++;
        } else {
          buffer += char;
          isInQuote = false;
        }
      } else {
        buffer += char;
        isInQuote = true;
      }
    } else if (char === PATH_DELIMITER) {
      if (isInQuote) {
        buffer += char;
      } else {
        // delimiter hit
        addBufferToResult();
      }
    } else {
      buffer += char;
    }
  }

  // TODO: what if we are still in a quote at this point?
  if (buffer.length) {
    addBufferToResult();
  }

  return result;
}

export function getEntityNameFromId(spaceId) {
  const splittedEntityId = spaceId.split('/');
  return splittedEntityId[splittedEntityId.length - 1];
}

export function isHomePage(pathname) {
  //TODO for some reasons if we open folder in home we will have spaces pathname, so we can't check correctly
  return pathname === '/home';
}

export function parseResourceId(pathname, username) {
  const parts = pathname.split('/');
  let resourceId = parts[2];
  if (parts.length === 2 || ['source', 'space'].indexOf(parts[1]) === -1) {
    resourceId = `"@${username}"`;
  }
  if (parts[3] === 'folder') {
    return [resourceId].concat(parts.slice(4)).join('.');
  }
  return resourceId;
}

export function getSourceNameFromResourceId(resourceId) {
  return resourceId.indexOf('.') > -1
    ? resourceId.slice(0, resourceId.indexOf('.'))
    : resourceId;
}

export function getResourceId(type, resourceId) {
  const hash = {
    home: HOME_SPACE_NAME,
    recent: RECENT_SPACE_NAME
  };
  return hash[type] || resourceId;
}

export function getEntityType(urlPath) {
  if (urlPath === '/') {
    return 'home';
  }
  const pathParts = urlPath.split('/');
  if (pathParts.length < 4) {
    return pathParts[1];
  }
  return pathParts[3];
}

export function getRootEntityType(urlPath) {
  if (!urlPath) {
    return undefined;
  }
  if (urlPath === '/') {
    return 'home';
  }
  const pathParts = urlPath.split('/');
  return pathParts[1];
}

const sql92words = new Set(['ABSOLUTE', 'ACTION', 'ADD', 'ALL', 'ALLOCATE', 'ALTER', 'AND', 'ANY', 'ARE', 'AS', 'ASC',
  'ASSERTION', 'AT', 'AUTHORIZATION', 'AVG', 'BEGIN', 'BETWEEN', 'BIT', 'BIT_LENGTH', 'BOTH', 'BY', 'CALL', 'CASCADE',
  'CASCADED', 'CASE', 'CAST', 'CATALOG', 'CHAR', 'CHAR_LENGTH', 'CHARACTER', 'CHARACTER_LENGTH', 'CHECK', 'CLOSE',
  'COALESCE', 'COLLATE', 'COLLATION', 'COLUMN', 'COMMIT', 'CONDITION', 'CONNECT', 'CONNECTION', 'CONSTRAINT',
  'CONSTRAINTS', 'CONTAINS', 'CONTINUE', 'CONVERT', 'CORRESPONDING', 'COUNT', 'CREATE', 'CROSS', 'CURRENT',
  'CURRENT_DATE', 'CURRENT_PATH', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURSOR', 'DATE', 'DAY',
  'DEALLOCATE', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DEFERRABLE', 'DEFERRED', 'DELETE', 'DESC', 'DESCRIBE',
  'DESCRIPTOR', 'DETERMINISTIC', 'DIAGNOSTICS', 'DISCONNECT', 'DISTINCT', 'DO', 'DOMAIN', 'DOUBLE', 'DROP', 'ELSE',
  'ELSEIF', 'END', 'ESCAPE', 'EXCEPT', 'EXCEPTION', 'EXEC', 'EXECUTE', 'EXISTS', 'EXIT', 'EXTERNAL', 'EXTRACT', 'FALSE',
  'FETCH', 'FIRST', 'FLOAT', 'FOR', 'FOREIGN', 'FOUND', 'FROM', 'FULL', 'FUNCTION', 'GET', 'GLOBAL', 'GO', 'GOTO',
  'GRANT', 'GROUP', 'HANDLER', 'HAVING', 'HOUR', 'IDENTITY', 'IF', 'IMMEDIATE', 'IN', 'INDICATOR', 'INITIALLY', 'INNER',
  'INOUT', 'INPUT', 'INSENSITIVE', 'INSERT', 'INT', 'INTEGER', 'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'ISOLATION',
  'JOIN', 'KEY', 'LANGUAGE', 'LAST', 'LEADING', 'LEAVE', 'LEFT', 'LEVEL', 'LIKE', 'LOCAL', 'LOOP', 'LOWER', 'MATCH',
  'MAX', 'MIN', 'MINUTE', 'MODULE', 'MONTH', 'NAMES', 'NATIONAL', 'NATURAL', 'NCHAR', 'NEXT', 'NO', 'NOT', 'NULL',
  'NULLIF', 'NUMERIC', 'OCTET_LENGTH', 'OF', 'ON', 'ONLY', 'OPEN', 'OPTION', 'OR', 'ORDER', 'OUT', 'OUTER', 'OUTPUT',
  'OVERLAPS', 'PAD', 'PARAMETER', 'PARTIAL', 'PATH', 'POSITION', 'PRECISION', 'PREPARE', 'PRESERVE', 'PRIMARY', 'PRIOR',
  'PRIVILEGES', 'PROCEDURE', 'PUBLIC', 'READ', 'REAL', 'REFERENCES', 'RELATIVE', 'REPEAT', 'RESIGNAL', 'RESTRICT',
  'RETURN', 'RETURNS', 'REVOKE', 'RIGHT', 'ROLLBACK', 'ROUTINE', 'ROWS', 'SCHEMA', 'SCROLL', 'SECOND', 'SECTION',
  'SELECT', 'SESSION', 'SESSION_USER', 'SET', 'SIGNAL', 'SIZE', 'SMALLINT', 'SOME', 'SPACE', 'SPECIFIC', 'SQL',
  'SQLCODE', 'SQLERROR', 'SQLEXCEPTION', 'SQLSTATE', 'SQLWARNING', 'SUBSTRING', 'SUM', 'SYSTEM_USER', 'TABLE',
  'TEMPORARY', 'THEN', 'TIME', 'TIMESTAMP', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE', 'TO', 'TRAILING', 'TRANSACTION',
  'TRANSLATE', 'TRANSLATION', 'TRIM', 'TRUE', 'UNDO', 'UNION', 'UNIQUE', 'UNKNOWN', 'UNTIL', 'UPDATE', 'UPPER', 'USAGE',
  'USER', 'USING', 'VALUE', 'VALUES', 'VARCHAR', 'VARYING', 'VIEW', 'WHEN', 'WHENEVER', 'WHERE', 'WHILE', 'WITH',
  'WORK', 'WRITE', 'YEAR', 'ZONE']);

export function constructFullPath(pathParts, preventQuoted, shouldEncode) {
  if (!pathParts) {
    return undefined;
  }
  const quotedPathParts = pathParts.map((part) => {
    let encodedPart;
    if (preventQuoted || part.match(/^[A-Z][A-Z0-9]*$/i) && !sql92words.has(part.toUpperCase())) {
      encodedPart = part;
    } else {
      encodedPart = '"' + part.replace(/"/g, '""') + '"';
    }
    return shouldEncode ? encodeURIComponent(encodedPart) : encodedPart;
  });
  return quotedPathParts.join('.');
}

export function constructFullPathAndEncode(pathParts) {
  return constructFullPath(pathParts, false, true);
}

export function constructResourcePath(fullPath, type = 'dataset') {
  return `/${type}/${fullPath}`;
}

export function getInitialResourceLocation(fullPath, datasetType, username) {
  return fullPath && fullPath[0] !== 'tmp' &&
         (datasetType === 'VIRTUAL_DATASET' || datasetType === 'PHYSICAL_DATASET_HOME_FILE')
         ? constructFullPath(fullPath, false)
         : `"@${username}"`;
}

export function getFullPathListFromEntity(entity) {
  const fileType = entity.get('fileType');
  switch (fileType) {
  case 'file':
    return entity.getIn(['fileFormat', 'fullPath']);
  case 'folder':
    return entity.get('fullPathList');
  case 'physicalDatasets':
  case 'dataset':
    return entity.getIn(['datasetConfig', 'fullPathList']);
  default:
    return [];
  }
}

export function getUniqueName(name, isUniqueFunc) {
  if (isUniqueFunc(name)) {
    return name;
  }

  let count = 1; // start at 1
  while (!isUniqueFunc(`${name} (${count})`)) {
    count++;
  }

  return `${name} (${count})`;
}

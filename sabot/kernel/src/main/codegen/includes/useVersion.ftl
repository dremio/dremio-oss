<#--

    Copyright (C) 2017-2019 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
/**
* USE BRANCH branchName
*/
SqlNode SqlUseBranch() :
{
  SqlParserPos pos;
  SqlIdentifier branchName;
}
{
  <USE> { pos = getPos(); }
  <BRANCH>
  branchName = SimpleIdentifier()
  {
    return new SqlUseBranch(pos, branchName);
  }
}

/**
* USE TAG tagName
*/
SqlNode SqlUseTag() :
{
  SqlParserPos pos;
  SqlIdentifier tagName;
}
{
  <USE> { pos = getPos(); }
  <TAG>
  tagName = SimpleIdentifier()
  {
    return new SqlUseTag(pos, tagName);
  }
}

/**
* USE COMMIT commitHash
*/
SqlNode SqlUseCommit() :
{
  SqlParserPos pos;
  SqlIdentifier commitHash;
}
{
  <USE> { pos = getPos(); }
  <COMMIT>
  commitHash = SimpleIdentifier()
  {
    return new SqlUseCommit(pos, commitHash);
  }
}

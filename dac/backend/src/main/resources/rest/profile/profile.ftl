<#--

    Copyright (C) 2017 Dremio Corporation

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

<#include "*/generic.ftl">
<#macro page_head>
<script src="/static/js/d3.v3.js"></script>
<script src="/static/js/dagre-d3.min.js"></script>
<script src="/static/js/graph.js"></script>
<script>
    var globalconfig = {
        "queryid" : "${model.getQueryId()}",
        "operators" : ${model.getOperatorsJSON()}
    };
</script>
</#macro>

<#macro page_body>
  <h3>Query and Planning</h3>
  <ul id="query-tabs" class="nav nav-tabs" role="tablist">
    <li><a href="#query-query" role="tab" data-toggle="tab">Query</a></li>
    <li><a href="#query-visual" role="tab" data-toggle="tab">Visualized Plan</a></li>
    <li><a href="#query-plan" role="tab" data-toggle="tab">Planning</a></li>
    <li><a href="#query-acceleration" role="tab" data-toggle="tab">Acceleration</a></li>
    <#if model.hasError() ><li><a href="#query-error" role="tab" data-toggle="tab">Error</a></li></#if>
  </ul>
  <div id="query-content" class="tab-content">
    <div id="query-query" class="tab-pane">
      <p><pre>${model.profile.query}</pre></p>
    </div>
    <div id="query-physical" class="tab-pane">
      <p><pre>${model.profile.plan}</pre></p>
    </div>
    <div id="query-visual" class="tab-pane">
      <svg id="query-visual-canvas" class="center-block"></svg>
    </div>
    <div id="query-acceleration" class="tab-pane">
      <h3>Summary</h3>
      <#if model.profile.hasAccelerationProfile() && model.profile.getAccelerationProfile().getAccelerated()>
        <p><pre>query was accelerated</pre></p>
      <#else>
        <p><pre>query was NOT accelerated</pre></p>
      </#if>
      <#if model.profile.hasAccelerationProfile()>
        <p>
        Number of Matching Substitutions:   ${model.getProfile().getAccelerationProfile().getNumSubstitutions()}
        <br>
        Time To Find Materializations:   ${model.getProfile().getAccelerationProfile().getMillisTakenGettingMaterializations()} ms
        <br>
        Time To Normalize:   ${model.getProfile().getAccelerationProfile().getMillisTakenNormalizing()} ms
        <br>
        Time To Substitute:   ${model.getProfile().getAccelerationProfile().getMillisTakenSubstituting()} ms
        </p>
      <#else>
        <p>
        Number of Matching Substitutions:   0
        <br>
        Time To Find Materializations:   --
        <br>
        Time To Normalize:   --
        <br>
        Time To Substitute:   --
        </p>
      </#if>
      <h3>Layout Details</h3>
        <#if model.profile.hasAccelerationProfile() && model.profile.getAccelerationProfile().getLayoutProfilesCount() != 0>
          <#list model.profile.getAccelerationProfile().getLayoutProfilesList() as layout>
          <p>
          <b>Layout Id:   ${layout.getLayoutId()}</b>
          <br>
          <b>Materialization Id:   ${layout.getMaterializationId()}</b>
          <br>
          Expiration Timestamp:   ${layout.getMaterializationExpirationTimestamp()}
          <br>
          Number of Times Selected/Used:  ${layout.getNumUsed()}
          <br>
          Number of Substitutions:   ${layout.getNumSubstitutions()}
          <br>
          Time To Substitute:   ${layout.getMillisTakenSubstituting()} ms
          <br>
          <i>Materialization Definition:</i>
          <p>Dimensions:
            <#list layout.getDimensionsList() as dim>
              ${dim},
            </#list>
          </p>
          <p>Measures:
            <#list layout.getMeasuresList() as measures>
              ${measures},
            </#list>
          </p>
          <p>Sorted:
            <#list layout.getSortedColumnsList() as sorted>
              ${sorted},
            </#list>
          </p>
          <p>Partitioned:
            <#list layout.getPartitionedColumnsList() as partitioned>
              ${partitioned},
            </#list>
          </p>
          <p>Distributed:
            <#list layout.getDistributionColumnsList() as dist>
              ${dist},
            </#list>
          </p>
          <p>Display:
            <#list layout.getDisplayColumnsList() as display>
              ${display},
            </#list>
          </p>
          <p>Original Materialization Plan:
            <pre>${layout.getPlan()}</pre>
          </p>
          <p>Normalized Materialization Plan:
            <#list layout.getNormalizedPlansList() as planNorm>
              <p><pre>${planNorm}</pre></p>
            </#list>
          </p>
          <p>Normalized Query Plan:
            <#list layout.getNormalizedQueryPlansList() as planNorm>
              <p><pre>${planNorm}</pre></p>
            </#list>
          </p>Substitutions:
            <#list layout.getSubstitutionsList() as substitution>
              <p><pre>${substitution.getPlan()}</pre></p>
            </#list>
          </p>
          </#list>
        <#else>
          <p>No materializations/layouts were applicable</p>
        </#if>
    </div>
    <div id="query-plan" class="tab-pane">
      <#if model.profile.getPlanPhasesCount() != 0>
        <#list model.profile.getPlanPhasesList() as planPhase>
        <h3>${planPhase.getPhaseName()} (${planPhase.getDurationMillis()} ms)</h3>
        <p><pre>${planPhase.getPlan()}</pre></p>
        </#list>
      <#else>
        <p>No planning phase information to show</p>
      </#if>
    </div>

    <#if model.hasError() >
    <div id="query-error" class="tab-pane">
      <p>
      <pre>
      ${model.getProfile().error?trim}
      </pre>
      </p>
      <p>Failure node: ${model.getProfile().errorNode}</p>
      <p>Error ID: ${model.getProfile().errorId}</p>

        <h3 class="panel-title">
          <a data-toggle="collapse" href="#error-verbose">
            Verbose Error Message...
          </a>
        </h3>
      <div id="error-verbose" class="panel-collapse collapse">
        <div class="panel-body">
        <p></p><p></p>
          <pre>
            ${model.getProfile().verboseError?trim}
          </pre>
        </div>
    </div>
    </#if>
  </div>

  <h3>Job Summary</h3>
  <dl class="dl-horizontal info-list">
    <dt>State:</dt>
    <dd>${model.getProfile().getState().name()}</dd>
    <dt>Coordinator:</dt>
    <dd>${model.getProfile().getForeman().getAddress()}</dd>
    <dt>Threads:</dt>
    <dd>${model.getProfile().getTotalFragments()}</dd>
    <dt>Planning Time:</dt>
    <dd>${model.getPlanningTime()}</dd>
  </dl>
  <h3>Threads</h3>
  <div class="panel-group" id="fragment-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#fragment-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="fragment-overview" class="panel-collapse collapse">
        <div class="panel-body">
          <svg id="fragment-overview-canvas" class="center-block"></svg>
          ${model.getFragmentsOverview()}
        </div>
      </div>
    </div>
    <#list model.getFragmentProfiles() as frag>
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#${frag.getId()}" class="collapsed">
            ${frag.getDisplayName()}
          </a>
        </h4>
      </div>
      <div id="${frag.getId()}" class="panel-collapse collapse">
        <div class="panel-body">
          ${frag.getContent()}
        </div>
      </div>
    </div>
    </#list>
  </div>

  <h3>Operators</h3>

  <div class="panel-group" id="operator-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#operator-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="operator-overview" class="panel-collapse collapse">
        <div class="panel-body">
          ${model.getOperatorsOverview()}
        </div>
      </div>
    </div>

    <#list model.getOperatorProfiles() as op>
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#${op.getId()}" class="collapsed">
            ${op.getDisplayName()}
          </a>
        </h4>
      </div>
      <div id="${op.getId()}" class="panel-collapse collapse">
        <div class="panel-body">
          ${op.getContent()}
          <div class="panel panel-default">
            <div class="panel-heading">
              <h4 class="panel-title">
                <a data-toggle="collapse" href="#${op.getId()}-metrics" class="collapsed">
                  Operator Metrics
                </a>
              </h4>
            </div>
            <div id="${op.getId()}-metrics" class="panel-collapse collapse">
              <div class="panel-body">
                ${op.getMetricsTable()}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    </#list>
  </div>
</#macro>

<@page_html/>

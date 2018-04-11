<#--

    Copyright (C) 2017-2018 Dremio Corporation

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

<#macro page_head>
</#macro>

<#macro page_body>
</#macro>

<#macro page_html>
  <!DOCTYPE html>
  <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta http-equiv="X-UA-Compatible" content="IE=edge">

      <title>Dremio</title>

      <link href="/static/css/bootstrap.min.css" rel="stylesheet">
      <link href="/static/css/styles.css" rel="stylesheet">

      <script src="/static/js/jquery.min.js"></script>
      <script src="/static/js/bootstrap.min.js"></script>

      <!-- HTML5 shim and Respond.js IE8 support of HTML5 elements and media queries -->
      <!--[if lt IE 9]>
        <script src="/static/js/html5shiv.js"></script>
        <script src="/static/js/1.4.2/respond.min.js"></script>
      <![endif]-->

      <@page_head/>
    </head>

    <body role="document">
      <div class="container-fluid" role="main">
        <@page_body/>
      </div>
    </body>
  </html>
</#macro>

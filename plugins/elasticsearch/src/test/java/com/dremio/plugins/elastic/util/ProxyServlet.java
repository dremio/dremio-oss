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
package com.dremio.plugins.elastic.util;

//========================================================================
//$Id: ProxyServlet.java 6208 2010-10-13 00:39:01Z gregw $
//Copyright 2004-2004 Mort Bay Consulting Pty. Ltd.
//------------------------------------------------------------------------
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//http://www.apache.org/licenses/LICENSE-2.0
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//========================================================================

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashSet;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.util.IO;

/**
* Proxy Servlet.
* <p>
* Forward requests to another server either as a standard web proxy (as defined by
* RFC2616) or as a transparent proxy.
*
*
* originally org.mortbay.servlet.ProxyServlet in jetty-util-6.1.26.jar
* forked to avoid GET being turned into post when content-type header is present
* see line 150
*
*/
public class ProxyServlet implements Servlet
{

 protected HashSet _DontProxyHeaders = new HashSet();
 {
     _DontProxyHeaders.add("proxy-connection");
     _DontProxyHeaders.add("connection");
     _DontProxyHeaders.add("keep-alive");
     _DontProxyHeaders.add("transfer-encoding");
     _DontProxyHeaders.add("te");
     _DontProxyHeaders.add("trailer");
     _DontProxyHeaders.add("proxy-authorization");
     _DontProxyHeaders.add("proxy-authenticate");
     _DontProxyHeaders.add("upgrade");
 }

 protected ServletConfig _config;
 protected ServletContext _context;

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
  */
 public void init(ServletConfig config) throws ServletException
 {
     this._config=config;
     this._context=config.getServletContext();
 }

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#getServletConfig()
  */
 public ServletConfig getServletConfig()
 {
     return _config;
 }

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#service(javax.servlet.ServletRequest, javax.servlet.ServletResponse)
  */
 public void service(ServletRequest req, ServletResponse res) throws ServletException,
         IOException
 {
     HttpServletRequest request = (HttpServletRequest)req;
     HttpServletResponse response = (HttpServletResponse)res;
     if ("CONNECT".equalsIgnoreCase(request.getMethod()))
     {
         handleConnect(request,response);
     }
     else
     {
         String uri=request.getRequestURI();
         if (request.getQueryString()!=null)
             uri+="?"+request.getQueryString();

         URL url=proxyHttpURL(request.getScheme(),
                 request.getServerName(),
                 request.getServerPort(),
                 uri);


         URLConnection connection = url.openConnection();
         connection.setAllowUserInteraction(false);

         // Set method
         HttpURLConnection http = null;
         if (connection instanceof HttpURLConnection)
         {
             http = (HttpURLConnection)connection;
             http.setRequestMethod(request.getMethod());
             http.setInstanceFollowRedirects(false);
         }

         // check connection header
         String connectionHdr = request.getHeader("Connection");
         if (connectionHdr!=null)
         {
             connectionHdr=connectionHdr.toLowerCase();
             if (connectionHdr.equals("keep-alive")||
                 connectionHdr.equals("close"))
                 connectionHdr=null;
         }

         // copy headers
         boolean xForwardedFor=false;
         boolean hasContent=false;
         Enumeration enm = request.getHeaderNames();
         while (enm.hasMoreElements())
         {
             // TODO could be better than this!
             String hdr=(String)enm.nextElement();
             String lhdr=hdr.toLowerCase();

             if (_DontProxyHeaders.contains(lhdr))
                 continue;
             if (connectionHdr!=null && connectionHdr.indexOf(lhdr)>=0)
                 continue;

             // This is were we changed from the original
             // calling connection.getOutputStream() turns the GET into a POST
             // we want hasContent to be false for GET
             if ("content-type".equals(lhdr) && ! "GET".equalsIgnoreCase(request.getMethod()))
                 hasContent=true;

             Enumeration vals = request.getHeaders(hdr);
             while (vals.hasMoreElements())
             {
                 String val = (String)vals.nextElement();
                 if (val!=null)
                 {
                     connection.addRequestProperty(hdr,val);
                     xForwardedFor|="X-Forwarded-For".equalsIgnoreCase(hdr);
                 }
             }
         }

         // Proxy headers
         connection.setRequestProperty("Via","1.1 (jetty)");
         if (!xForwardedFor)
         {
             connection.addRequestProperty("X-Forwarded-For",
                     request.getRemoteAddr());
             connection.addRequestProperty("X-Forwarded-Proto",
                     request.getScheme());
             connection.addRequestProperty("X-Forwarded-Host",
                     request.getServerName());
             connection.addRequestProperty("X-Forwarded-Server",
                     request.getLocalName());
         }

         // a little bit of cache control
         String cache_control = request.getHeader("Cache-Control");
         if (cache_control!=null &&
             (cache_control.indexOf("no-cache")>=0 ||
              cache_control.indexOf("no-store")>=0))
             connection.setUseCaches(false);

         // customize Connection

         try
         {
             connection.setDoInput(true);

             // do input thang!
             InputStream in=request.getInputStream();
             if (hasContent)
             {
                 connection.setDoOutput(true);
                 IO.copy(in,connection.getOutputStream());
             }

             // Connect
             connection.connect();
         }
         catch (Exception e)
         {
             _context.log("proxy",e);
         }

         InputStream proxy_in = null;

         // handler status codes etc.
         int code=500;
         if (http!=null)
         {
             proxy_in = http.getErrorStream();

             code=http.getResponseCode();
             response.setStatus(code, http.getResponseMessage());
         }

         if (proxy_in==null)
         {
             try {proxy_in=connection.getInputStream();}
             catch (Exception e)
             {
                 _context.log("stream",e);
                 proxy_in = http.getErrorStream();
             }
         }

         // clear response defaults.
         response.setHeader("Date",null);
         response.setHeader("Server",null);

         // set response headers
         int h=0;
         String hdr=connection.getHeaderFieldKey(h);
         String val=connection.getHeaderField(h);
         while(hdr!=null || val!=null)
         {
             String lhdr = hdr!=null?hdr.toLowerCase():null;
             if (hdr!=null && val!=null && !_DontProxyHeaders.contains(lhdr))
                 response.addHeader(hdr,val);

             h++;
             hdr=connection.getHeaderFieldKey(h);
             val=connection.getHeaderField(h);
         }
         response.addHeader("Via","1.1 (jetty)");

         // Handle
         if (proxy_in!=null)
             IO.copy(proxy_in,response.getOutputStream());

     }
 }

 /* ------------------------------------------------------------ */
 /** Resolve requested URL to the Proxied URL
  * @param scheme The scheme of the received request.
  * @param serverName The server encoded in the received request(which
  * may be from an absolute URL in the request line).
  * @param serverPort The server port of the received request (which
  * may be from an absolute URL in the request line).
  * @param uri The URI of the received request.
  * @return The URL to which the request should be proxied.
  * @throws MalformedURLException
  */
 protected URL proxyHttpURL(String scheme, String serverName, int serverPort, String uri)
     throws MalformedURLException
 {
     return new URL(scheme,serverName,serverPort,uri);
 }

 /* ------------------------------------------------------------ */
 public void handleConnect(HttpServletRequest request,
                           HttpServletResponse response)
     throws IOException
 {
     String uri = request.getRequestURI();

     String port = "";
     String host = "";

     int c = uri.indexOf(':');
     if (c>=0)
     {
         port = uri.substring(c+1);
         host = uri.substring(0,c);
         if (host.indexOf('/')>0)
             host = host.substring(host.indexOf('/')+1);
     }




     InetSocketAddress inetAddress = new InetSocketAddress (host, Integer.parseInt(port));

     //if (isForbidden(HttpMessage.__SSL_SCHEME,addrPort.getHost(),addrPort.getPort(),false))
     //{
     //    sendForbid(request,response,uri);
     //}
     //else
     {
         InputStream in=request.getInputStream();
         OutputStream out=response.getOutputStream();

         Socket socket = new Socket(inetAddress.getAddress(),inetAddress.getPort());

         response.setStatus(200);
         response.setHeader("Connection","close");
         response.flushBuffer();

         IO.copyThread(socket.getInputStream(),out);
         IO.copy(in,socket.getOutputStream());
     }
 }




 /* (non-Javadoc)
  * @see javax.servlet.Servlet#getServletInfo()
  */
 public String getServletInfo()
 {
     return "Proxy Servlet";
 }

 /* (non-Javadoc)
  * @see javax.servlet.Servlet#destroy()
  */
 public void destroy()
 {

 }
 /**
  * Transparent Proxy.
  *
  * This convenience extension to AsyncProxyServlet configures the servlet
  * as a transparent proxy.   The servlet is configured with init parameter:<ul>
  * <li> ProxyTo - a URI like http://host:80/context to which the request is proxied.
  * <li> Prefix  - a URI prefix that is striped from the start of the forwarded URI.
  * </ul>
  * For example, if a request was received at /foo/bar and the ProxyTo was  http://host:80/context
  * and the Prefix was /foo, then the request would be proxied to http://host:80/context/bar
  *
  */
 public static class Transparent extends ProxyServlet
 {
     String _prefix;
     String _proxyTo;

     public Transparent()
     {
     }

     public Transparent(String prefix, String server, int port)
     {
         _prefix=prefix;
         _proxyTo="http://"+server+":"+port;
     }

     public void init(ServletConfig config) throws ServletException
     {
         if (config.getInitParameter("ProxyTo")!=null)
             _proxyTo=config.getInitParameter("ProxyTo");
         if (config.getInitParameter("Prefix")!=null)
             _prefix=config.getInitParameter("Prefix");
         if (_proxyTo==null)
             throw new UnavailableException("No ProxyTo");
         super.init(config);

         _context.log("Transparent ProxyServlet @ "+(_prefix==null?"-":_prefix)+ " to "+_proxyTo);

     }

     protected URL proxyHttpURL(final String scheme, final String serverName, int serverPort, final String uri) throws MalformedURLException
     {
         if (_prefix!=null && !uri.startsWith(_prefix))
             return null;

         if (_prefix!=null)
             return new URL(_proxyTo+uri.substring(_prefix.length()));
         return new URL(_proxyTo+uri);
     }
 }

}

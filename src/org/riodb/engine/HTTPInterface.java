/*
 	Copyright (c) 2021 Lucio D Matos,  www.riodb.org
 
    This file is part of RioDB
    
    RioDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    RioDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    A copy of the GNU General Public License should be found in the root
    directory. If not, see <https://www.gnu.org/licenses/>.
 
*/

/*

	HTTP interface opens the API access to submit statement requests to RIODB
	If HTTP interface is disabled (.conf file), then RioDB will rely solely on
	the initial SQL files, without access to handle subsequent requests
	via API. 
	
	This class configures the HTTP listener for the RioDB API as specified in
	the .conf file loaded during start up.  

*/

package org.riodb.engine;

import java.io.BufferedReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.riodb.sql.SQLExecutor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.BasicAuthenticator;
import com.sun.net.httpserver.HttpContext;

@SuppressWarnings("restriction")
public class HTTPInterface {

	// http server and https server
	private static HttpServer httpServer = null;
	private static HttpsServer httpsServer = null;

	// max timeout for select statement (could become a .conf parameter passed to
	// constructor)
	private int selectStmtTimeout = 60;

	// setter for timeout
	public void setTimeout(int newTimeout) {
		selectStmtTimeout = newTimeout;
	}

	// getter for timeout
	public int getTimeout() {
		return selectStmtTimeout;
	}

	// Method for starting an HTTP end point (NO SSL)
	public boolean startHttp(int port) {
		boolean success = false;
		try {

			InetAddress localHost = InetAddress.getLoopbackAddress();
			InetSocketAddress sockAddr = new InetSocketAddress(localHost, port);

			// this is restricted to localhost
			httpServer = HttpServer.create(sockAddr, 0);

			// this is not restricted to localhost
			// httpServer = HttpServer.create(new InetSocketAddress(port), 0);

			httpServer.createContext("/", new RootHandler());
			httpServer.createContext("/rio", new RioHandler());
			httpServer.setExecutor(null); // creates a default executor
			RioDB.rio.getSystemSettings().getLogger().info("Starting HTTP interface on " + port);
			httpServer.start();
			success = true;
		} catch (IOException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Error starting HTTP interface");
			RioDB.rio.getSystemSettings().getLogger().error(e.getMessage());
			httpServer = null;
			return false;
		}
		return success;
	}

	// Method for starting an HTTPS end point
	public boolean startHttps(String keystoreFilename, String keyStorePasswd, int port) {
		boolean success = false;
		try {

			httpsServer = HttpsServer.create(new InetSocketAddress(port), 0);

			// SSL Context
			SSLContext sslContext;
			sslContext = SSLContext.getInstance("TLSv1.2");

			// KeyStore
			char[] password = keyStorePasswd.toCharArray();
			KeyStore ks = KeyStore.getInstance("JKS");
			FileInputStream fis = new FileInputStream(keystoreFilename);
			ks.load(fis, password);

			// setup the key manager factory
			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ks, password);

			// setup the trust manager factory
			TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(ks);

			// setup the HTTPS context and parameters
			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
				public void configure(HttpsParameters params) {
					try {
						// initialize the SSL context
						SSLContext context = getSSLContext();
						SSLEngine engine = context.createSSLEngine();
						params.setNeedClientAuth(false);
						params.setCipherSuites(engine.getEnabledCipherSuites());
						params.setProtocols(engine.getEnabledProtocols());

						// Set the SSL parameters
						SSLParameters sslParameters = context.getSupportedSSLParameters();
						params.setSSLParameters(sslParameters);

					} catch (Exception ex) {
						RioDB.rio.getSystemSettings().getLogger().error("Failed to create HTTPS port");
					}
				}
			});

			httpsServer.createContext("/", new RootHandler());
			// httpsServer.createContext("/rio", new RioHandler());

			HttpContext hc1 = httpsServer.createContext("/rio", new RioHandler());
			if (RioDB.rio.getUserMgr() != null) {
				hc1.setAuthenticator(new BasicAuthenticator("get") {
					@Override
					public boolean checkCredentials(String user, String pwd) {
						// block requests posing as SYSTEM.
						if (user == null || user.equals("SYSTEM")) {
							return false;
						}
						return RioDB.rio.getUserMgr().authenticate(user, pwd);
					}
				});
			}
			httpsServer.setExecutor(null);
			RioDB.rio.getSystemSettings().getLogger().info("Starting HTTPsServer on " + port);
			httpsServer.start();
			success = true;
		} catch (NoSuchAlgorithmException | IOException | KeyStoreException | CertificateException
				| UnrecoverableKeyException | KeyManagementException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Error starting HTTPS Interface.");
			RioDB.rio.getSystemSettings().getLogger().error(e.getMessage());
			httpsServer = null;
			success = false;
		}
		return success;
	}

	// The handler class that will reply calls to /rio
	static class RioHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange t) throws IOException {

			// default response
			String response = "{\"status\": 200, \"message\": \"RioDB here. Tell me WHEN.\"}\n";
			if (t.getRequestMethod().equals("POST")) {
				String stmt = parseRequestBody(t.getRequestBody());
				if (stmt != null && stmt.length() > 0) {

					String userName = null;
					if (RioDB.rio.getUserMgr() != null) {
						HttpPrincipal p = t.getPrincipal();
						userName = p.getUsername();
						if (userName != null) {
							userName = userName.toUpperCase();
						}
					}

						// Send the statement and username to the SQLExecutor class
						// true for persistStament,  true for respondWithDetails
						response = SQLExecutor.execute(stmt, userName, true, true) + "\n";
						
				} 
			}
			t.sendResponseHeaders(200, response.getBytes().length);
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}

		// Convert input stream to statement string
		private final String parseRequestBody(InputStream inputStream) {

			String requestPayload = inputStreamToString(inputStream).trim();
			if (requestPayload != null && requestPayload.length() > 2) {
				if (requestPayload.charAt(0) == '\"' && requestPayload.charAt(requestPayload.length() - 1) == '\"') {
					requestPayload = requestPayload.substring(1);
					requestPayload = requestPayload.substring(0, requestPayload.length() - 1);
					return requestPayload;
				}
			}
			return null;
		}
	}

	// Default handler for URL root level. (The other handler responds to URL /rio
	static class RootHandler implements HttpHandler {
		@Override
		public void handle(final HttpExchange t) throws IOException {
			String response = "{\"status\": 200, \"message\":\"RioDB here. Tell me WHEN.\"}\n";
			t.sendResponseHeaders(200, response.getBytes().length);
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}

	// Convert input stream to string
	private static String inputStreamToString(InputStream inputStream) {

		String response = "";
		StringBuilder textBuilder = new StringBuilder();
		Reader reader = new BufferedReader(
				new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())));
		int c = 0;
		try {
			while ((c = reader.read()) != -1) {
				textBuilder.append((char) c);
			}
			response = textBuilder.toString();
		} catch (IOException e) {
			RioDB.rio.getSystemSettings().getLogger().error("Error converting inputStream to String");
			RioDB.rio.getSystemSettings().getLogger().error(e.getMessage());
		}

		return response;

	}

	// stop HTTP - For shutdown only. Once stopped, there's no way to submit a
	// start-up request.
	public static void stop() {
		if (httpServer != null) {
			httpServer.stop(0);
			httpServer = null;
			RioDB.rio.getSystemSettings().getLogger().info("Stopped HTTP interface");
		}
		if (httpsServer != null) {
			httpsServer.stop(0);
			httpsServer = null;
			RioDB.rio.getSystemSettings().getLogger().info("Stopped HTTPS interface");
		}

	}

}

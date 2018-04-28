package com.jacamars.dsp.crosstalk.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;


import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.session.SessionHandler;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.crosstalk.manager.Crosstalk;

/**
 * Creates the HTTP handler for the Minimal server. This is an adaptation of the
 * Jacamar's Inc. prototype web server. This source code is licensed under
 * Apache Commons 2.
 * 
 * @author Ben M. Faul.
 *
 */

public class WebAccess implements Runnable {
	/** A uuid to use with the web access */
	public static final String uuid = "crosstalk:api";
	
	/** Thread this runs on */
	Thread me;
	
	/**  default HTTP port */
	protected int port = 7379;
	
	/** Object mapper for command access */
	static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	/**
	 * Creates the default Server
	 * 
	 * @param args
	 *            . String[]. Args[0] contains the name of the users file, if
	 *            not, presume "users.json"
	 * @throws Exception
	 *             on network or JSON parsing errors.
	 */
	public static void main(String[] args) throws Exception {
		new WebAccess(7379);
	}

	/**
	 * Creates the WebMQ instance and starts it. The Jetty side receives
	 * ajax calls for Mosaic actions.
	 * 
	 * @param port int. THe port to listen on.
	 *          
	 * @throws Exception
	 *             or network errors.
	 */
	public WebAccess(int port) throws Exception {
		this.port = port;
				
		me = new Thread(this);
		me.start();
		
		Thread.sleep(500);
	}
	
	/**
	 * Starts the JETTY server
	 */
	 public void run() {
		Server server = new Server(port);
		Handler handler = new Handler();
		SessionHandler sh = new SessionHandler(); // org.eclipse.jetty.server.session.SessionHandler
		sh.setHandler(handler);

		server.setHandler(sh); // set session handle
		try {
			server.start();
			server.join();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 

}

/**
 * The class that handles HTTP calls for API actions.
 * 
 * @author Ben M. Faul
 *
 */

@MultipartConfig
class Handler extends AbstractHandler {
	/**
	 * The property for temp files.
	 */
	
	protected static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	SessionHandler sh = new SessionHandler(); // org.eclipse.jetty.server.session.SessionHandler

	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {

		response.addHeader("Access-Control-Allow-Origin", "*");
		response.addHeader("Access-Control-Allow-Headers","Content-Type");

		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);

		HttpSession session = request.getSession();
		String userAgent = request.getHeader("User-Agent");
		
		
		int code = 200;
		String html = "";

		InputStream body = request.getInputStream();
		baseRequest.setHandled(true);

		// ///////////////////////////////////////////////////////

		session.setAttribute("path", target);
		String type = request.getContentType();
		String ipAddress = getIpAddress(request);
		
		if (target.equals("/status")) {
			baseRequest.setHandled(true);
			response.getWriter().println("OK");
			response.setStatus(200);
			return;
		}
		
		if (target.equals("/api")) {
			response.setContentType("application/json;charset=utf-8");
			baseRequest.setHandled(true);
			
			try {
				String result =  executeCommand(getIpAddress(request),body);
				response.getWriter().println(result);
			} catch (Exception error) {
				error.printStackTrace();
				response.setStatus(500);
				response.getWriter().println(error.toString());
			}
			baseRequest.setHandled(true);
			return;
		}
		
		if (target.equals("/log")) {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("text/html;charset=utf-8");
			baseRequest.setHandled(true);
			
			Deque<String> logDeck = new ArrayDeque<String>();
			Crosstalk.deqeues.add(logDeck);
			String contents = null;
			response.getWriter().println("<html><h1>Web Logging Starting...</h1>");
			response.flushBuffer();
			while(true) {
				contents = logDeck.poll();
				if (contents != null) { 
					
					try {
						response.getWriter().println(contents);
						response.flushBuffer();
					} catch (Exception error) {
						error.printStackTrace();
						synchronized(Crosstalk.deqeues) {
							Crosstalk.deqeues.remove(logDeck);
						}
						return;
					}
				} else {
					try {
						Thread.sleep(250);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		

		baseRequest.setHandled(true);
		response.setStatus(404);
	}
	
	///////////////////////////
	
	String executeCommand(String ip, InputStream body) throws Exception {
		byte [] bytes = new byte[4096];
		int rc = body.read(bytes);
		if (rc <= 0) {
			return "{\"error\":true, \"message\": \"empty command\"}";
		}
		String content = new String(bytes,0,rc);
		String results = ApiCommand.instantiate(ip,content).toJson();
		return results;
	}

	/**
	 * Return the IP address of this
	 * 
	 * @param request
	 *            HttpServletRequest. The web browser's request object.
	 * @return String the ip:remote-port of this browswer's connection.
	 */
	public String getIpAddress(HttpServletRequest request) {
		String ip = request.getHeader("x-forwarded-for");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}
		ip += ":" + request.getRemotePort();
		return ip;
	}
}
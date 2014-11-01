import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

public class DiscReqProc implements Runnable {

	private static final Logger rootLogger = LogManager.getRootLogger();

	private Socket sock;
	private DataStore ds;
	private HTTPRequestLine reqLine;

	/**
	 * ReqProc for Discovery Server
	 */
	public DiscReqProc(Socket sock, DataStore ds) {
		this.sock = sock;
		this.ds = ds;
		reqLine = null;
	}

	/**
	 * Run method for request processor for DataServer threads
	 */
	public void run() {
		BufferedReader in = null;
		String line;
		try {
			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));

			line = in.readLine().trim();
			rootLogger.trace("Received line: " + line);
			reqLine = HTTPRequestLineParser.parse(line);
			if (reqLine == null)
				rootLogger.trace("URI: null");

			// If everything is in order
			if (reqLine != null && reqLine.getURI().equalsIgnoreCase("tweets")) {

				// GET request
				if (reqLine.getMethod().equalsIgnoreCase("GET")) {
					String minServer = ds.getMinServerLoad();
					sendDataServerRequest(minServer);
				}

				// POST request
				else if (reqLine.getMethod().equalsIgnoreCase("POST") 
						&& reqLine.getParameters().containsKey("me")) {
					String newServer = reqLine.getParameters().get("me");
					
					if (newServer.length() == 0) {
						String responseheaders = "HTTP/1.1 400 Bad Request\n";
						returnHeaderOnly(responseheaders);
					}
					
					HashMap<String, ArrayList<String>> serverMap = ds
							.getServerLoadList(newServer);
					ArrayList<String> minServerList = serverMap.get("min");
					String minServer = "";
					if(minServerList.size() > 0) {
							minServer = minServerList.get(0);
					}
					ArrayList<String> servers = serverMap.get("list");
					sendDiscRequest(newServer, servers, minServer);
				}

				// Neither GET nor POST request
				else {
					String responseheaders = "HTTP/1.1 400 Bad Request\n";
					rootLogger.trace("Bad Request");
					returnHeaderOnly(responseheaders);
				}
			}

			// If everything is not in order
			else {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				if (reqLine != null) {
					responseheaders = "HTTP/1.1 404 Not Found\n";
				}
				returnHeaderOnly(responseheaders);
			}

		} catch (IOException e) {
			String responseheaders = "HTTP/1.1 500 Internal Server Error\n";
			rootLogger.trace("IOException occurred");
			try {
				returnHeaderOnly(responseheaders);
			} catch (IOException e1) {
				rootLogger.trace("returnHeadersOnly failed");
			}
		}

		try {
			in.close();
			sock.close();
		} catch (IOException e) {
			rootLogger.trace("Problem closing socket");
		}

		rootLogger.trace("Finished Working\n");
	}
	
	
	
	/**
	 * Method for handling Discovery GET requests
	 */
	@SuppressWarnings("unchecked")
	private void sendDataServerRequest(String minServer) throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("min", minServer);
		String responsebody = obj.toJSONString();
		String responseheaders = "HTTP/1.1 200 OK\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		rootLogger.trace("Sending Request to FE: "
				+ responseheaders.trim() + " with body: "
				+ responsebody);
		OutputStream out = sock.getOutputStream();
		out.write(responseheaders.getBytes());
		out.write(responsebody.getBytes());
		out.flush();
		out.close();
	}
	

	/**
	 * Method for handling Discovery POST requests
	 */
	@SuppressWarnings("unchecked")
	private void sendDiscRequest(String newServer, ArrayList<String> servers,
			String minServer) {

			int count = 0;
			JSONObject obj = new JSONObject();
			String responsebody = "";
			String requestheaders = "";
			for (String server : servers) {
				obj.put("new", newServer);
				if (server.equalsIgnoreCase(minServer)) {
					obj.put("min", true);
				} else {
					obj.put("min", false);
				}
				
				responsebody = obj.toJSONString();
				requestheaders = "POST /tweets?d=disc HTTP/"
						+ reqLine.getVersion() + "\nContent-Length: "
						+ responsebody.getBytes().length + "\n\n";
				
				String[] serverParts = server.split(":");
				Socket dataSock = null;
				try {
					dataSock = new Socket(serverParts[0],
							Integer.parseInt(serverParts[1]));
					OutputStream out = dataSock.getOutputStream();
					rootLogger.trace("Sending Request to DataStore: "
							+ requestheaders.trim() + " with body: "
							+ responsebody);

					out.write(requestheaders.getBytes());
					out.write(responsebody.getBytes());
				
					String lineText = "";
	
					BufferedReader in = new BufferedReader(
							new InputStreamReader(dataSock.getInputStream()));
	
					lineText = in.readLine().trim();
					rootLogger
							.trace("Received from DataStore: " + lineText);
					if (lineText.equalsIgnoreCase("HTTP/1.1 200 OK\n")) {
						count += 1;
					}
	
					out.flush();
					out.close();
					in.close();
					dataSock.close();
					if (count == 3) {
						String responseheaders = "HTTP/1.1 200 OK\n";
						returnHeaderOnly(responseheaders);
						count += 1;
					}
				} catch (NumberFormatException | IOException e) {
					rootLogger.trace("Bad Server: " + server);
				}
			}
			if (count < 3) {
				String responseheaders = "HTTP/1.1 200 OK\n";
				try {
					returnHeaderOnly(responseheaders);
				} catch (IOException e) {
					rootLogger.trace("returnHeadersOnly failed");
				}
			}
	}

	/**
	 * Generalized method for returning messages that only require a header
	 */
	private void returnHeaderOnly(String responseheaders) throws IOException {
		rootLogger.trace("Sending: " + responseheaders);
		OutputStream out = sock.getOutputStream();
		out.write(responseheaders.getBytes());
		out.write(("\n").getBytes());
		out.flush();
		out.close();
	}
}

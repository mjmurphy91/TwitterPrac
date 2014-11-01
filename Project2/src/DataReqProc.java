import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class DataReqProc implements Runnable {
	
	private static final Logger rootLogger = LogManager.getRootLogger();

	private Socket sock;
	private DataStore ds;
	private JSONParser parser;
	private HTTPRequestLine reqLine;
	
	
	/**
	 * ReqProc for Data Server
	 */
	public DataReqProc(Socket sock, DataStore ds) {
		this.sock = sock;
		this.ds = ds;
		parser = new JSONParser();
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
			
			String jsonText = "";
			
			line = in.readLine().trim();
			rootLogger.trace("Received line: " + line);
			reqLine = HTTPRequestLineParser.parse(line);
			if (reqLine == null)
				rootLogger.trace("URI: null");
			else if (reqLine.getMethod().equalsIgnoreCase("POST")
					|| reqLine.getMethod().equalsIgnoreCase("CONNECT")) {
				line = in.readLine();
				int bufferSize = Integer.parseInt(line.split(" ")[1]);
				rootLogger.trace("Buffer Size: " + bufferSize);
				line = in.readLine();
				char[] bytes = new char[bufferSize];
				in.read(bytes, 0, bufferSize);
				jsonText = new String(bytes);
			}
			
			rootLogger.trace("JsonText: " + jsonText);

			//If everything is in order
			if(reqLine != null && reqLine.getURI().equalsIgnoreCase("tweets")) {
				
				//DATA GET request
				if (reqLine.getMethod().equalsIgnoreCase("GET")
						&& reqLine.getParameters().containsKey("v")
						&& reqLine.getParameters().containsKey("q")) {
					getDataRequest();
				}
				
				//POST request
				else if (reqLine.getMethod().equalsIgnoreCase("POST")) {
					//Discovery Server adding server
					if(reqLine.getParameters().containsKey("d")) {
						recvDiscRequest(jsonText);
					}
					//Fellow Data Server sending a snapshot
					else if(reqLine.getParameters().containsKey("s")) {
						recvSnapshot(jsonText);
					}
					//Fellow Data Server sharing an update
					else if(reqLine.getParameters().containsKey("u")) {
						//TODO: update
					}
					//FE server sending tweet
					else {
						postDataRequest(jsonText);
					}	
				}
				
				//Neither GET nor POST nor UPDATE request
				else {
					String responseheaders = "HTTP/1.1 400 Bad Request\n";
					rootLogger.trace("Bad Request");
					returnHeaderOnly(responseheaders);
				}
			} 
			
			//If everything is not in order
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
	 * Method for handling DATA GET requests
	 */
	@SuppressWarnings("unchecked")
	private void getDataRequest() throws IOException {
		int version = Integer.parseInt(reqLine.getParameters().get("v"));
		String hashtag = reqLine.getParameters().get("q");
		rootLogger.trace("Received GET request with hashtag: "
				+ hashtag + " and version: " + version);

		int currentVersion = ds.getVersion(hashtag);
		if(version != currentVersion) {
			ArrayList<String> tweets = ds.getTweet(hashtag);
			JSONObject obj = new JSONObject();
			obj.put("q", hashtag);
			obj.put("v", currentVersion);
			obj.put("tweets", tweets);

			String responsebody = obj.toJSONString();
			String responseheaders = "HTTP/1.1 200 OK\n" + "Content-Length: "
					+ responsebody.getBytes().length + "\n\n";
			OutputStream out = sock.getOutputStream();
			out.write(responseheaders.getBytes());
			out.write(responsebody.getBytes());
			out.flush();
			out.close();
			
		} else {
			String responseheaders = "HTTP/1.1 304 Not Modified\n";
			returnHeaderOnly(responseheaders);
		}
	}
	
	
	/**
	 * Method for handling DATA POST requests
	 */
	@SuppressWarnings("unchecked")
	private void postDataRequest(String jsonText) throws IOException {
		JSONObject obj = null;
		try {
			obj=(JSONObject) parser.parse(jsonText);
			
			if (obj == null) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				rootLogger.trace("Bad Request");
				returnHeaderOnly(responseheaders);
			}

			else {
				ArrayList<String> hashtags = (ArrayList<String>) obj
						.get("hashtags");
				String tweet = (String) obj.get("tweet");
				if (hashtags == null || hashtags.size() == 0 || tweet == null
						|| tweet.length() == 0) {
					if (obj.containsKey("servers") && obj.containsKey("data")
							&& obj.containsKey("vers")) {
						ds.setSnapshot(obj);
						String responseheaders = "HTTP/1.1 201 Created\n";
						returnHeaderOnly(responseheaders);
					} else {
						String responseheaders = "HTTP/1.1 400 Bad Request\n";
						returnHeaderOnly(responseheaders);
					}
				}

				else {
					for (String hashtag : hashtags) {
						ds.addTweet(hashtag, tweet, -1);
					}
					String responseheaders = "HTTP/1.1 201 Created\n";
					returnHeaderOnly(responseheaders);
				}
			}
		} catch (ParseException e) {
			rootLogger.trace("position: " + e.getPosition());
			rootLogger.trace(e);
		}
	}
	
	
	/**
	 * Method for handling Discovery GET requests
	 */
	private void recvDiscRequest(String jsonText) throws IOException {
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(jsonText);

			if (obj == null) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				rootLogger.trace("Bad Request");
				returnHeaderOnly(responseheaders);
			}

			else {
				String newServer = (String) obj.get("new");
				boolean isMin = (boolean) obj.get("min");
				if (newServer == null || newServer.length() == 0) {
					String responseheaders = "HTTP/1.1 400 Bad Request\n";
					returnHeaderOnly(responseheaders);
				}

				else {
					ds.addServer(newServer);
					String responseheaders = "HTTP/1.1 201 Created\n";
					returnHeaderOnly(responseheaders);

					if (isMin) {
						// Send snapshot to newServer if you're min server
						String responsebody = ds.getSnapshot().toJSONString();
						String requestheaders = "POST /tweets HTTP/"
								+ reqLine.getVersion() + "\nContent-Length: "
								+ responsebody.getBytes().length + "\n\n";
						String[] serverParts = newServer.split(":");
						Socket dataSock = new Socket(serverParts[0],
								Integer.parseInt(serverParts[1]));
						OutputStream out = dataSock.getOutputStream();
						rootLogger.trace("Sending Request to DataStore: "
								+ requestheaders.trim() + " with body: "
								+ responsebody);

						out.write(requestheaders.getBytes());
						out.write(responsebody.getBytes());

						String lineText = "";

						// Receive response from DataServer
						BufferedReader in = new BufferedReader(
								new InputStreamReader(dataSock.getInputStream()));

						lineText = in.readLine().trim();
						rootLogger.trace("Received from DataStore: " + lineText);

						out.flush();
						out.close();
						in.close();
						dataSock.close();
					}
				}
			}
		} catch (ParseException e) {
			rootLogger.trace("position: " + e.getPosition());
			rootLogger.trace(e);
		}
	}
	
	
	/**
	 * Method for receiving a fellow Data Server's snapshot
	 */
	private void recvSnapshot(String jsonText) throws IOException {
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(jsonText);
			ds.setSnapshot(obj);
			String responseheaders = "HTTP/1.1 201 Created\n";
			returnHeaderOnly(responseheaders);
		} catch (ParseException e) {
			rootLogger.trace("position: " + e.getPosition());
			rootLogger.trace(e);
			String responseheaders = "HTTP/1.1 400 Bad Request\n";
			rootLogger.trace("Bad Request");
			returnHeaderOnly(responseheaders);
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

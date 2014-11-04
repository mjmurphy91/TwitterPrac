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
					|| reqLine.getMethod().equalsIgnoreCase("GET")) {
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
					getDataRequest(jsonText);
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

					//FE server sending tweet or fellow Data Server sharing an 
					//update
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
	private void getDataRequest(String jsonText) throws IOException {
		int version = Integer.parseInt(reqLine.getParameters().get("v"));
		String hashtag = reqLine.getParameters().get("q");
		rootLogger.trace("Received GET request with hashtag: "
				+ hashtag + " and version: " + version);

		int currentVersion = ds.getVersion(hashtag);
		HashMap<String, String> vectors = ds.getServerVectoList();
		
		JSONObject obj;
		try {
			obj = (JSONObject) parser.parse(jsonText);
			boolean vectorsNeedUpate = true;
		
			// Needs update if its vector timestamp is less than the Data 
			// Server's for all servers
			HashMap<String, String> FEvectors = (HashMap<String, String>) obj
					.get("vec");
			for (String server : vectors.keySet()) {
				if (FEvectors.containsKey(server)) {
					if (Integer.parseInt(vectors.get(server)) < 
							Integer.parseInt(FEvectors.get(server))) {
						rootLogger.trace("FE vector: " + FEvectors.get(server)
								+ " was greater than Data vector: " 
								+ vectors.get(server) + " for server: " 
								+ server);
						vectorsNeedUpate = false;
						break;
					}
				}
			}

			if(version != currentVersion && vectorsNeedUpate) {
				ArrayList<String> tweets = ds.getTweet(hashtag);
				obj = new JSONObject();
				obj.put("q", hashtag);
				obj.put("v", currentVersion);
				obj.put("tweets", tweets);
				obj.put("vec", vectors);
	
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
		} catch (ParseException | NullPointerException e) {
			e.printStackTrace();
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
			
			if (obj == null || !obj.containsKey("hashtags")) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				rootLogger.trace("Bad Request");
				returnHeaderOnly(responseheaders);
			}

			else {
				ArrayList<String> hashtags = (ArrayList<String>) obj
						.get("hashtags");
				String tweet = (String) obj.get("tweet");
				HashMap<String, String> info; 
				String self = null;
				String myVector = null;
				int count = 0;
				
				if(reqLine.getParameters().containsKey("u")) {
					String theirSelf = (String) obj.get("self");
					String theirVector = (String) obj.get("vector");
					
					info = ds.addTweets(hashtags, tweet, -1, null, false, theirSelf, theirVector);
					if(info != null) {
						self = info.get("self");
						info.remove("self");
						info.remove(self);
						info.remove(theirSelf);
						myVector = theirVector;
						self = theirSelf;
					}
					else {
						info = new HashMap<String, String>();
					}
					String responseheaders = "HTTP/1.1 201 Created\n";
					returnHeaderOnly(responseheaders);
				}
				else {
					info = ds.addTweets(hashtags, tweet, -1, null, true, null, null);
					self = info.get("self");
					myVector = info.get(self);
					info.remove("self");
					info.remove(self);
				}
				
				for(String server: info.keySet()) {
					// Send request to DataServer
					String[] serverInfo = server.split(":");
					Socket dataSock = null;
					try {
						dataSock = new Socket(serverInfo[0], Integer.parseInt(serverInfo[1]));
					} catch (IOException e) {
						ds.removeServer(server);
						continue;
					}
					
					JSONObject obj2 = new JSONObject();
					obj2.put("self", self);
					obj2.put("vector", myVector);
					obj2.put("tweet", tweet);
					obj2.put("hashtags", hashtags);
					String responsebody = obj2.toJSONString();
					String requestheaders = "POST /tweets?u=update HTTP/" + reqLine.getVersion()
							+ "\nContent-Length: " + responsebody.getBytes().length
							+ "\n\n";
					OutputStream out = dataSock.getOutputStream();
					rootLogger.trace("Sending Request to DataStore: " + requestheaders.trim() +
							" with body: " + responsebody);
					
					out.write(requestheaders.getBytes());
					out.write(responsebody.getBytes());

					String lineText = "";

					// Receive response from DataServer
					BufferedReader in = new BufferedReader(new InputStreamReader(
							dataSock.getInputStream()));

					lineText = in.readLine().trim();
					rootLogger.trace("Received from DataStore: " + lineText);
					
					if (lineText.equalsIgnoreCase("HTTP/1.1 201 Created\n")) {
						count += 1;
					}

					out.flush();
					out.close();
					in.close();
					dataSock.close();
					
					if (count == 3 && !reqLine.getParameters().containsKey("u")) {
						String responseheaders = "HTTP/1.1 201 Created\n";
						returnHeaderOnly(responseheaders);
						count += 1;
					}
				}
				if (count < 3 && !reqLine.getParameters().containsKey("u")) {
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
	 * Method for handling Discovery requests
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

					if (isMin) {
						// Send snapshot to newServer if you're min server
						String responsebody = ds.getSnapshot().toJSONString();
						String requestheaders = "POST /tweets?s=snap HTTP/"
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
					
					String responseheaders = "HTTP/1.1 200 OK\n";
					returnHeaderOnly(responseheaders);
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

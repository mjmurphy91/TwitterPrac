import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class ReqProc implements Runnable {

	private static final Logger rootLogger = LogManager.getRootLogger();

	private Socket sock;
	private Socket dataSock;
	private DataStore ds;
	private JSONParser parser;
	private HTTPRequestLine reqLine;
	private boolean isDataProc;
	private boolean isDiscProc;
	
	/**
	 * ReqProc for Discovery Server
	 */
	public ReqProc(Socket sock, DataStore ds) {
		this.sock = sock;
		this.ds = ds;
		parser = new JSONParser();
		reqLine = null;
		isDataProc = false;
		isDiscProc = true;
	}

	/**
	 * ReqProc for Data Server
	 */
	public ReqProc(Socket sock, DataStore ds, String discServer) {
		this.sock = sock;
		this.ds = ds;
		parser = new JSONParser();
		reqLine = null;
		isDataProc = true;
		isDiscProc = false;
	}
	
	/**
	 * ReqProc for FE Server
	 */
	public ReqProc(Socket sock, DataStore ds, String dataStoreIP,
			String dataStorePort, String discServer) {
		String responseheaders = "";
		this.sock = sock;
		try {
			dataSock = new Socket(dataStoreIP, Integer.parseInt(dataStorePort));
		} catch (NumberFormatException e) {
			rootLogger.trace("PORT: " + dataStorePort + " could not be parsed to an int");
			responseheaders = "HTTP/1.1 500 Internal Server Error\n";
			try {
				returnHeaderOnly(responseheaders);
			} catch (IOException e1) {
				rootLogger.trace("returnHeaderOnly failed");
			}
		} catch (UnknownHostException e) {
			rootLogger.trace("Cannot connect to given host");
			responseheaders = "HTTP/1.1 500 Internal Server Error\n";
			try {
				returnHeaderOnly(responseheaders);
			} catch (IOException e1) {
				rootLogger.trace("returnHeaderOnly failed");
			}
		} catch (IOException e) {
			rootLogger.trace("IOException occurred");
			responseheaders = "HTTP/1.1 500 Internal Server Error\n";
			try {
				returnHeaderOnly(responseheaders);
			} catch (IOException e1) {
				rootLogger.trace("returnHeaderOnly failed");
			}
		}
		
		parser = new JSONParser();
		this.ds = ds;
		reqLine = null;
		isDataProc = false;
		isDiscProc = false;
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
				if (isDataProc && reqLine.getMethod().equalsIgnoreCase("GET") 
						&& reqLine.getParameters().containsKey("v")
						&& reqLine.getParameters().containsKey("q")) {
					getDataRequest();
				}
				
				//DATA GET request
				else if (!isDataProc && reqLine.getMethod().equalsIgnoreCase("GET") 
						&& reqLine.getParameters().containsKey("q")) {
					getFERequest();
				}
				
				//POST request
				else if (reqLine.getMethod().equalsIgnoreCase("POST")) {
					if (isDiscProc) {
						String[] fullIP = sock.getInetAddress().toString().split("/");
						String IP = fullIP[fullIP.length - 1];
						int PORT = sock.getPort();
						String newServer = IP + ":" + PORT;
						//HashMap<String, ArrayList<String>> serverMap = ds.getServerLoadList();
						//ds.addServerLoad(newServer);
						
						//String minServer = serverMap.get("min").get(0);
						//ArrayList<String> servers = serverMap.get("list");
						//sendDiscRequest(jsonText, servers, minServer);
					}
					else if (isDataProc) {
						//Discovery Server adding server
						if(reqLine.getParameters().containsKey("d")) {
							recvDiscRequest(jsonText);
						}
						//Fellow Data Server sending a snapshot
						else if(reqLine.getParameters().containsKey("s")) {
							//snapshot
						}
						//Fellow Data Server sharing an update
						else if(reqLine.getParameters().containsKey("u")) {
							//update
						}
						//FE server sending tweet
						else {
							postDataRequest(jsonText);
						}
					}
					else {
						postFERequest(jsonText);
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
	 * Method for handling FE GET requests
	 */
	@SuppressWarnings("unchecked")
	private void getFERequest() throws IOException {
		String hashtag = reqLine.getParameters().get("q");
		int version = ds.getVersion(hashtag);
		
		//Send request to DataServer
		String requestheaders = "GET /tweets?q=" + hashtag + "&v=" + version
				+ " HTTP/" + reqLine.getVersion() + "\n";
		OutputStream out = dataSock.getOutputStream();
		out.write(requestheaders.getBytes());
		//out.write(("\n").getBytes());
		
		String line;
		String lineText = "";
		
		//Receive response from DataServer
		BufferedReader in = new BufferedReader(new InputStreamReader(
				dataSock.getInputStream()));
		
		line = in.readLine().trim();
		rootLogger.trace("Received line: " + line);
		if (line.equalsIgnoreCase("HTTP/1.1 200 OK")) {
			line = in.readLine();
			int bufferSize = Integer.parseInt(line.split(" ")[1]);
			rootLogger.trace("Buffer Size: " + bufferSize);
			line = in.readLine();
			char[] bytes = new char[bufferSize];
			in.read(bytes, 0, bufferSize);
			lineText = new String(bytes);
		}
		else {
			lineText = line;
		}
		rootLogger.trace("Received lineText: " + lineText);
		
		//If ds not up to date, update it
		if(!lineText.trim().equalsIgnoreCase("HTTP/1.1 304 Not Modified")) {
			rootLogger.trace("Version for " + hashtag + " was not current");
			JSONObject obj = null;
			try {
				obj = (JSONObject) parser.parse(lineText);
			} catch (ParseException e) {
				rootLogger.trace("position: " + e.getPosition());
				rootLogger.trace(e);
			}
			
			if (obj == null || obj.get("tweets") == null) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				returnHeaderOnly(responseheaders);
			}
			
			else {
				ArrayList<String> newTweets = (ArrayList<String>) obj.get("tweets");
				int newVersion = (((Long) obj.get("v")).intValue());
				for (String tweet : newTweets) {
					ds.addTweet(hashtag, tweet, newVersion);
				}
			}
		}
		else {
			rootLogger.trace("Version is current, reading from ds");
		}
		
		out.flush();
		out.close();
		in.close();
		rootLogger.trace("Closed DataServer socket connections");
		
		//Respond with ds values
		ArrayList<String> tweets = ds.getTweet(hashtag);
		JSONObject obj2 = new JSONObject();
		obj2.put("q", hashtag);
		obj2.put("tweets", tweets);
		rootLogger.trace("Sending tweets: " + tweets);

		String responsebody = obj2.toJSONString();
		String responseheaders = "HTTP/1.1 200 OK\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		OutputStream out2 = sock.getOutputStream();
		out2.write(responseheaders.getBytes());
		out2.write(responsebody.getBytes());
		//out2.write(("\n").getBytes());
		out2.flush();
		out2.close();
		rootLogger.trace("Closed Client socket connection");
	}

	/**
	 * Method for handling FE POST requests
	 */
	@SuppressWarnings("unchecked")
	private void postFERequest(String jsonText) throws IOException {
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(jsonText);
		} catch (ParseException e) {
			rootLogger.trace("position: " + e.getPosition());
			rootLogger.trace(e);
		}
		if (obj == null || obj.get("text") == null) {
			String responseheaders = "HTTP/1.1 400 Bad Request\n";
			returnHeaderOnly(responseheaders);
		}
		
		else {
			String tweet = (String) obj.get("text");
			ArrayList<String> hashtags = getHashtags(tweet);
			
			if (hashtags.size() == 0 || hashtags.contains(" ") 
					|| hashtags.contains("")) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				returnHeaderOnly(responseheaders);
			}

			else {
				// Send request to DataServer
				JSONObject obj2 = new JSONObject();
				obj2.put("tweet", tweet);
				obj2.put("hashtags", hashtags);
				String responsebody = obj2.toJSONString();
				String requestheaders = "POST /tweets HTTP/" + reqLine.getVersion()
						+ "\nContent-Length: " + responsebody.getBytes().length
						+ "\n\n";
				OutputStream out = dataSock.getOutputStream();
				rootLogger.trace("Sending Request to DataStore: " + requestheaders.trim() +
						" with body: " + responsebody);
				
				out.write(requestheaders.getBytes());
				out.write(responsebody.getBytes());
				//out.write(("\n").getBytes());

				String lineText = "";

				// Receive response from DataServer
				BufferedReader in = new BufferedReader(new InputStreamReader(
						dataSock.getInputStream()));

				lineText = in.readLine().trim();
				rootLogger.trace("Received from DataStore: " + lineText);

				out.flush();
				out.close();
				in.close();

				String responseheaders = lineText + "\n";
				returnHeaderOnly(responseheaders);
			}
		}
	}
	
	/**
	 * Method for handling Discovery GET requests
	 */
	private void recvDiscRequest(String jsonText) throws IOException {
		JSONObject obj = null;
		try {
			obj=(JSONObject) parser.parse(jsonText);
			
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
					
					if(isMin) {
						//Send snapshot to newServer if you're min server
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
						rootLogger.trace("Received from DataStore: " 
								+ lineText);
						
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
	 * Method for handling Discovery POST requests
	 */
	@SuppressWarnings("unchecked")
	private void sendDiscRequest(String jsonText, ArrayList<String> servers, 
			String minServer) throws IOException {
		try {
			JSONObject obj=(JSONObject) parser.parse(jsonText);
			
			if (obj == null) {
				String responseheaders = "HTTP/1.1 400 Bad Request\n";
				rootLogger.trace("Bad Request");
				returnHeaderOnly(responseheaders);
			}

			else {
				String newServer = (String) obj.get("me");
				if (newServer == null || newServer.length() == 0) {
					String responseheaders = "HTTP/1.1 400 Bad Request\n";
					returnHeaderOnly(responseheaders);
				}

				else {
					int count = 0;
					JSONObject obj2 = new JSONObject();
					obj2.put("new", newServer);
					String responsebody = obj2.toJSONString();
					String requestheaders = "POST /tweets?d=disc HTTP/"
							+ reqLine.getVersion() + "\nContent-Length: "
							+ responsebody.getBytes().length + "\n\n";
					for (String server : servers) {
						// Send request to each DataServer
						if(server.equalsIgnoreCase(minServer)) {
							obj2.put("min", true);
						}
						else {
							obj2.put("min", false);
						}
						String[] serverParts = server.split(":");
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
					}
					if (count < 3) {
						String responseheaders = "HTTP/1.1 200 OK\n";
						returnHeaderOnly(responseheaders);
					}
				}
			}
		} catch (ParseException e) {
			rootLogger.trace("position: " + e.getPosition());
			rootLogger.trace(e);
		}
	}
	
	/**
	 * Extracts the hashtags in a given tweet
	 */
	private ArrayList<String> getHashtags(String tweets) {
		ArrayList<String> hashtags = new ArrayList<String>();
		String[] words = tweets.split(" ");
		for(String word : words) {
			if(word.startsWith("#")) {
				hashtags.add(word.substring(1));
			}
		}
		return hashtags;
	}
}

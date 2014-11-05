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


public class FEReqProc implements Runnable {

	private static final Logger rootLogger = LogManager.getRootLogger();

	private Socket sock;
	private Socket dataSock;
	private DataStore ds;
	private JSONParser parser;
	private HTTPRequestLine reqLine;
	
	/**
	 * ReqProc for FE Server
	 */
	public FEReqProc(Socket sock, DataStore ds, String[] dataServer, String discServer) {
		this.sock = sock;
		this.dataSock = null;

		//Guarantee connection to a Data Server if at least one is still up
		while(dataSock == null) {
			String[] dataServerParts = dataServer[0].split(":");
			try {
				dataSock = new Socket(dataServerParts[0], Integer.parseInt(dataServerParts[1]));
				System.out.println("Connected to: " + dataServerParts[0] + ":" + dataServerParts[1]);
			} catch (NumberFormatException | IOException e) {
				rootLogger.trace("Problem with server: " + dataServerParts[0]);
				dataSock = null;
				String server = getDataServer(discServer, dataServer[0]);
				if(server.split(":").length < 2) {
					dataServer[0] = "Bad:1";
				}
				else {
					dataServer[0] = server;
					ds.updateSelf(server);
				}
			}
		}
		
		parser = new JSONParser();
		this.ds = ds;
		reqLine = null;
	}
	
	
	/**
	 * Run method for request processor for FE threads
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
				
				//GET request
				if (reqLine.getMethod().equalsIgnoreCase("GET") 
						&& reqLine.getParameters().containsKey("q")) {
					getFERequest();
				}
				
				//POST request
				else if (reqLine.getMethod().equalsIgnoreCase("POST")) {
					postFERequest(jsonText);
				}
				
				//Neither GET nor POST request
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
	 * Method for handling FE GET requests
	 */
	@SuppressWarnings("unchecked")
	private void getFERequest() throws IOException {
		String hashtag = reqLine.getParameters().get("q");
		int version = ds.getVersion(hashtag);
		HashMap<String, String> vectors = ds.getServerVectoList();
		
		//Send request to DataServer
		JSONObject obj = new JSONObject();
		obj.put("vec", vectors);
		String requestbody = obj.toJSONString();
		String requestheaders = "GET /tweets?q=" + hashtag + "&v=" + version
				+ " HTTP/" + reqLine.getVersion() + "\nContent-Length: " 
				+ requestbody.getBytes().length + "\n\n";
		OutputStream out = dataSock.getOutputStream();
		out.write(requestheaders.getBytes());
		out.write(requestbody.getBytes());		
		
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
			obj = null;
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
				HashMap<String, String> newVectors = (HashMap<String, String>) obj.get("vec");
				ArrayList<String> hashtags = new ArrayList<String>();
				hashtags.add(hashtag);
				boolean first = true;
				for (String tweet : newTweets) {
					if(first) {
						ds.addTweets(hashtags, tweet, newVersion, newVectors, false, null, null);
						first = false;
					}
					else {
						ds.addTweets(hashtags, tweet, newVersion, null, false, null, null);
					}
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
	 * Contacts Discovery Server to find out who its new Data Server is
	 */
	private String getDataServer(String discServer, String badServer) {
		String[] discParts = discServer.split(":");
		String lineText = "";
		Socket discSock;
		try {
			//Send request to DiscServer
			String IP = discParts[0];
			int PORT = Integer.parseInt(discParts[1]);
			discSock = new Socket(IP, PORT);
			String requestheaders = "GET /tweets?b=" + badServer + " HTTP/1.1\n";
			
			rootLogger.trace("Sending Request to DiscServer: " + requestheaders.trim());

			OutputStream out = discSock.getOutputStream();
			out.write(requestheaders.getBytes());
			

			// Receive response from DiscServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					discSock.getInputStream()));

			while (!(line = in.readLine().trim()).equals("")) {
				if(line.equalsIgnoreCase("HTTP/1.1 200 OK")) {
					line = in.readLine();
					int bufferSize = Integer.parseInt(line.split(" ")[1]);
					line = in.readLine();
					char[] bytes = new char[bufferSize];
					in.read(bytes, 0, bufferSize);
					lineText = new String(bytes);
					break;
				}
				else {
					lineText = line;
				}
			}	
			
			rootLogger.trace("Received from DiscServer: " + lineText);
			
			out.flush();
			out.close();
			in.close();
			discSock.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lineText;
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

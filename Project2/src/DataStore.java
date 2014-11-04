import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DataStore {
	private static final Logger rootLogger = LogManager.getRootLogger();

	private HashMap<String, Integer> serverVectors;
	private String self;
	private HashMap<String, ArrayList<String>> dataMap;
	private HashMap<String, Integer> dataVersionMap;
	private ReadWriteLock lock;
	private HashMap<String, Integer> serverLoads;
	private HashMap<String, HashMap<Integer, ArrayList<String>>> queue;

	public DataStore(String self) {
		this.self = self;
		serverVectors = new HashMap<String, Integer>();
		serverVectors.put(self, 0);
		dataMap = new HashMap<String, ArrayList<String>>();
		dataVersionMap = new HashMap<String, Integer>();
		lock = new ReentrantReadWriteLock();
		queue = new HashMap<String, HashMap<Integer, ArrayList<String>>>();
	}

	public DataStore() {
		serverLoads = new HashMap<String, Integer>();
		lock = new ReentrantReadWriteLock();
	}
	
	public void updateSelf(String newSelf) {
		rootLogger.trace("Received request to change self to: " + newSelf);
		lock.writeLock().lock();
		rootLogger.trace("Acquired updateSelf Lock");
		self = newSelf;
		lock.writeLock().unlock();
		rootLogger.trace("Relinquished updateSelf Lock");
	}
	

	/**
	 * Returns a copy of the array corresponding to the given hashtag, or null
	 * if hashtag is not in the map
	 */
	public ArrayList<String> getTweet(String hashtag) {
		if (dataMap == null) {
			rootLogger.trace("DataMap is null");
			return null;
		}

		rootLogger.trace("Tweets with hashtag: " + hashtag + " requested");
		ArrayList<String> tweets = new ArrayList<String>();
		lock.readLock().lock();
		rootLogger.trace("Acquired getTweet Lock");
		if (dataMap.keySet().contains(hashtag)) {
			tweets.addAll(dataMap.get(hashtag));
		}
		lock.readLock().unlock();
		rootLogger.trace("Relinquished getTweet Lock");
		return tweets;
	}

	/**
	 * Returns current version number of given hashtag, or 0 if given hashtag is
	 * not in the DataStore
	 */
	public int getVersion(String hashtag) {
		if (dataVersionMap == null) {
			rootLogger.trace("DataVersionMap is null");
			return -1;
		}

		int version = 0;
		rootLogger.trace("Request for verion of hashtag: " + hashtag
				+ " requested");
		lock.readLock().lock();
		rootLogger.trace("Acquired getVersion Lock");
		if (dataVersionMap.keySet().contains(hashtag)) {
			version = dataVersionMap.get(hashtag);
		}
		lock.readLock().unlock();
		rootLogger.trace("Relinquished getVersion Lock");
		return version;
	}

	/**
	 * Adds given tweet to the array corresponding to the given hashtag. Creates
	 * a new array if this hashtag does not exist yet
	 */
	public HashMap<String, String> addTweets(ArrayList<String> hashtags, String tweet,
			int newVersion, HashMap<String, String> newServerVectors,
			boolean isFromFEPost, String theirSelf, String theirVector) {
		if (dataMap == null) {
			rootLogger.trace("DataMap is null");
			return null;
		}

		rootLogger.trace("Request to add tweet: " + tweet);
		lock.writeLock().lock();
		rootLogger.trace("Acquired addTweets Lock");
		ArrayList<String> tweets;
		int version;
		
		//Update from Front End
		if (theirSelf == null && theirVector == null) {
			for (String hashtag : hashtags) {
				if(newServerVectors != null && isFromFEPost == false) {
					emptyHashtag(hashtag);
				}
				if (dataMap.keySet().contains(hashtag)) {
					tweets = dataMap.get(hashtag);
					tweets.add(tweet);
					version = dataVersionMap.get(hashtag);
					if (newVersion < version) {
						dataVersionMap.put(hashtag, version + 1);
					} else {
						dataVersionMap.put(hashtag, newVersion);
					}
				} else {
					tweets = new ArrayList<String>();
					tweets.add(tweet);
					dataVersionMap.put(hashtag, 1);
				}
				dataMap.put(hashtag, tweets);
			}
		}
		
		//Update from another Data Server
		else {
			if (serverVectors.containsKey(theirSelf)) {
				if (serverVectors.get(theirSelf) >= Integer.parseInt(theirVector)) {
					return null;
				}
				else {
					if (Integer.parseInt(theirVector) > (1 + serverVectors.get(theirSelf))) {
						enqueue(theirSelf, theirVector, tweet, hashtags);
						return null;
					}
					else {
						serverVectors.put(theirSelf, Integer.parseInt(theirVector));
						for (String hashtag : hashtags) {
							if (dataMap.keySet().contains(hashtag)) {
								tweets = dataMap.get(hashtag);
								tweets.add(tweet);
								version = dataVersionMap.get(hashtag);
								if (newVersion < version) {
									dataVersionMap.put(hashtag, version + 1);
								} else {
									dataVersionMap.put(hashtag, newVersion);
								}
							} else {
								tweets = new ArrayList<String>();
								tweets.add(tweet);
								dataVersionMap.put(hashtag, 1);
							}
							dataMap.put(hashtag, tweets);
						}
						dequeue(theirSelf, theirVector);
					}
				}
			}
			
			else {
				if(Integer.parseInt(theirVector) > 1) {
					enqueue(theirSelf, theirVector, tweet, hashtags);
					lock.writeLock().unlock();
					rootLogger.trace("Relinquished addTweet Lock and enqueueing");
					return null;
				}
				serverVectors.put(theirSelf, Integer.parseInt(theirVector));
				for (String hashtag : hashtags) {
					if (dataMap.keySet().contains(hashtag)) {
						tweets = dataMap.get(hashtag);
						tweets.add(tweet);
						version = dataVersionMap.get(hashtag);
						if (newVersion < version) {
							dataVersionMap.put(hashtag, version + 1);
						} else {
							dataVersionMap.put(hashtag, newVersion);
						}
					} else {
						tweets = new ArrayList<String>();
						tweets.add(tweet);
						dataVersionMap.put(hashtag, 1);
					}
					dataMap.put(hashtag, tweets);
				}
				dequeue(theirSelf, theirVector);
			}
			lock.writeLock().unlock();
			rootLogger.trace("Relinquished addTweet Lock");
			return null;
		}

		if(isFromFEPost) {
			int vector = serverVectors.get(self);
			serverVectors.put(self, vector + 1);
			rootLogger.trace("Updating vector for: " + self + " to: "
					+ (vector + 1));
		}
		
		if (newServerVectors != null) {
			for (String server : newServerVectors.keySet()) {
				serverVectors.put(server, Integer.parseInt(newServerVectors.get(server)));
			}
		}
		HashMap<String, String> returnInfo = new HashMap<String, String>();
		returnInfo.put("self", self);
		for (String original : serverVectors.keySet()) {
			returnInfo.put(original, serverVectors.get(original).toString());
		}

		lock.writeLock().unlock();
		rootLogger.trace("Relinquished addTweet Lock");
		return returnInfo;
	}
	
	/**
	 * Method for storing data that arrived too early in a queue to be gotten
	 * later
	 */
	private void enqueue(String theirSelf, String theirVector, String tweet,
			ArrayList<String> hashtags) {
		if (queue.containsKey(theirSelf)) {
			HashMap<Integer, ArrayList<String>> vectorHash = queue
					.get(theirSelf);
			if (!vectorHash.containsKey(Integer.parseInt(theirVector))) {
				ArrayList<String> queueStrings = new ArrayList<String>();
				queueStrings.add(tweet);
				queueStrings.addAll(hashtags);
				vectorHash.put(Integer.parseInt(theirVector), queueStrings);
				queue.put(theirSelf, vectorHash);
			}
		} else {
			ArrayList<String> queueStrings = new ArrayList<String>();
			queueStrings.add(tweet);
			queueStrings.addAll(hashtags);
			HashMap<Integer, ArrayList<String>> vectorHash 
				= new HashMap<Integer, ArrayList<String>>();
			vectorHash.put(Integer.parseInt(theirVector), queueStrings);
			queue.put(theirSelf, vectorHash);
		}
	}

	/**
	 * Method for adding data that arrived too early and placed in a queue into
	 * the main data structure
	 */
	private void dequeue(String theirSelf, String theirVector) {
		if(queue.containsKey(theirSelf)) {
			HashMap<Integer, ArrayList<String>> vectorHash = queue.get(theirSelf);
			int vector = Integer.parseInt(theirVector);
			while(true) {
				if(vectorHash.containsKey(vector+1)) {
					vector = vector + 1;
					ArrayList<String> queueStrings = vectorHash.get(vector);
					String tweet = queueStrings.get(0);
					ArrayList<String> hashtags = new ArrayList<String>(); 
					hashtags.addAll(1, queueStrings);
					serverVectors.put(theirSelf, vector);
					ArrayList<String> tweets;
					
					for (String hashtag : hashtags) {
						if (dataMap.containsKey(hashtag)) {
							tweets = dataMap.get(hashtag);
							tweets.add(tweet);
							int version = dataVersionMap.get(hashtag);
							dataVersionMap.put(hashtag, version + 1);
						} else {
							tweets = new ArrayList<String>();
							tweets.add(tweet);
							dataVersionMap.put(hashtag, 1);
						}
						dataMap.put(hashtag, tweets);
					}
				}
				else {
					for(int i = 0; i < vector; i++) {
						if(vectorHash.containsKey(i)) {
							vectorHash.remove(i);
						}
					}
					break;
				}
			}
		}
	}

	/**
	 * Returns a copy of the serverVectors List
	 */
	public HashMap<String, String> getServerVectoList() {
		if (serverVectors == null) {
			rootLogger.trace("ServerList is null");
			return null;
		}

		rootLogger.trace("Copy of serverList requested");
		HashMap<String, String> serverVectorcpy = new HashMap<String, String>();
		lock.readLock().lock();
		rootLogger.trace("Acquired getServerList Lock");
		for (String original : serverVectors.keySet()) {
			serverVectorcpy.put(original, serverVectors.get(original).toString());
		}
		lock.readLock().unlock();
		rootLogger.trace("Relinquished getServerList Lock");
		return serverVectorcpy;
	}
	
	public String getMinServerLoad() {
		
		if (serverLoads == null) {
			rootLogger.trace("ServerLoads is null");
			return null;
		}
		lock.writeLock().lock();
		rootLogger.trace("Obtained getMinServerLoad Lock");

		String minServer = "";
		int min = 10000;
		for (String server : serverLoads.keySet()) {
			if (serverLoads.get(server) < min) {
				min = serverLoads.get(server);
				minServer = server;
			}
		}
		//Increase load of minServer
		if(!minServer.isEmpty()) {
			int load = serverLoads.get(minServer);
			serverLoads.put(minServer, load + 1);
			rootLogger.trace("Increased load of: " + minServer);
		}
		
		lock.writeLock().unlock();
		rootLogger.trace("Relinquished getMinServerLoad Lock");
		return minServer;
	}
	

	/**
	 * If newServer is not already present, returns a copy of the servers in
	 * serverLoads and adds newServer.
	 */
	public ArrayList<String> getServerLoadList(String newServer) {
		rootLogger.trace("Trying to add newServer: " + newServer);
		if (serverLoads == null) {
			rootLogger.trace("ServerLoads is null");
			return null;
		}

		rootLogger.trace("getServerLoadList requested");
		ArrayList<String> serverListcpy = new ArrayList<String>();
		lock.writeLock().lock();
		rootLogger.trace("Acquired getServerLoadList Lock");
		ArrayList<String> temp = new ArrayList<String>();

		if (!serverLoads.containsKey(newServer)) {
			for (String server : serverLoads.keySet()) {
				temp.add(server);
			}
			
			int size = temp.size();
			for(int i = 0; i < size; i++) {
				String minServer = "";
				int min = 10000;
				for (String server : temp) {
					if (serverLoads.get(server) < min) {
						min = serverLoads.get(server);
						minServer = server;
					}
				}
				serverListcpy.add(minServer);
				temp.remove(minServer);
			}
				
			//Increase load of minServer
			if(!serverListcpy.isEmpty()) {
				int load = serverLoads.get(serverListcpy.get(0));
				serverLoads.put(serverListcpy.get(0), load + 1);
				rootLogger.trace("Increased load of: " + serverListcpy.get(0));
			}
			
			serverLoads.put(newServer, 0);
		}

		lock.writeLock().unlock();
		rootLogger.trace("Relinquished getServerLoadList Lock");
		return serverListcpy;
	}

	/**
	 * DiscoveryServer refreshing list of servers
	 */
	public void addServer(String newServer) {
		if (dataMap == null) {
			rootLogger.trace("ServerList is null");
		}

		else {
			lock.writeLock().lock();
			rootLogger.trace("Acquired addServer Lock");
			if (!serverVectors.containsKey(newServer)) {
				serverVectors.put(newServer, 0);
			}
			lock.writeLock().unlock();
			rootLogger.trace("Relinquished addServer Lock");
		}
	}

	/**
	 * Sets DataStore's entire content to given snapshot
	 */
	@SuppressWarnings("unchecked")
	public void setSnapshot(JSONObject snapshot) {
		if (dataMap == null) {
			rootLogger.trace("DataMap is null");
		}

		else {
			rootLogger.trace("Updating server with snapshot");
			lock.writeLock().lock();
			rootLogger.trace("Acquired setSnapshot Lock");
			
			// Set ServerList
			JSONObject newServers =  (JSONObject) snapshot.get("servers");
			serverVectors = new HashMap<String, Integer>();
			serverVectors.put(self, 0);
			for (Object obServer : newServers.keySet()) {
				String server = (String) obServer;
				int newVector = Integer.parseInt((String) newServers.get(server));
				serverVectors.put(server, newVector);
			}
			
			// Set DataMap
			JSONObject newData =  (JSONObject) snapshot.get("data");
			dataMap = new HashMap<String, ArrayList<String>>();
			for (Object obData : newData.keySet()) {
				String data = (String) obData;
				ArrayList<String> newDataList = new ArrayList<String>();
				newDataList.addAll((ArrayList<String>) newData.get(data));
				dataMap.put(data, newDataList);
			}
			
			// Set DataVersionMap
			JSONObject newVers =  (JSONObject) snapshot.get("vers");
			dataVersionMap = new HashMap<String, Integer>();
			for (Object obData : newVers.keySet()) {
				String data = (String) obData;
				int newVersion = Integer.parseInt((String) newVers.get(data));
				dataVersionMap.put(data, newVersion);
			}
			
			
			lock.writeLock().unlock();
			rootLogger.trace("Relinquished setSnapshot Lock");
		}
	}

	/**
	 * Returns a snapshot of the DataStore's entire content
	 */
	@SuppressWarnings("unchecked")
	public JSONObject getSnapshot() {
		if (dataMap == null) {
			rootLogger.trace("DataMap is null");
			return null;
		}

		JSONObject obj = new JSONObject();
		rootLogger.trace("Request for snapshot");
		lock.readLock().lock();
		rootLogger.trace("Acquired getSnapshot Lock");

		// Get ServerList
		JSONObject serverListcpy = new JSONObject();
		for (String original : serverVectors.keySet()) {
			serverListcpy.put(original, serverVectors.get(original).toString());
		}
		obj.put("servers", serverListcpy);

		// Get DataMap
		JSONObject dataMapcpy = new JSONObject();
		JSONArray temp2;
		for (String key : dataMap.keySet()) {
			temp2 = new JSONArray();
			temp2.addAll(dataMap.get(key));
			dataMapcpy.put(key, temp2);
		}
		obj.put("data", dataMapcpy);

		// Get DataVersionMap
		JSONObject dataVersionMapcpy = new JSONObject();
		String temp3;
		for (String key : dataVersionMap.keySet()) {
			temp3 = dataVersionMap.get(key).toString();
			dataVersionMapcpy.put(key, temp3);
		}
		obj.put("vers", dataVersionMapcpy);

		lock.readLock().unlock();
		rootLogger.trace("Relinquished getSnapshot Lock");
		return obj;
	}
	
	/**
	 * Method for removing a server from the server list
	 */
	public void removeServer(String server) {
		lock.writeLock().lock();
		rootLogger.trace("Acquired removeServer Lock");
		if(serverVectors == null) {
			serverLoads.remove(server);
		}
		else {
			serverVectors.remove(server);
		}
		lock.writeLock().unlock();
		rootLogger.trace("Relinquished removeServer Lock");
	}
	
	/**
	 * FE method which empties the dataMap of the given hashtag so it can be 
	 * updated
	 */
	private void emptyHashtag(String hashtag) {
		dataMap.remove(hashtag);
	}

	/**
	 * Seeds DataStore for testing purposes
	 */
	public void seed(String server, int vector, ArrayList<String> hashtags, 
			String tweet, int newVersion) {
		
		ArrayList<String> data = new ArrayList<String>();
		data.add(tweet);
		for(String hashtag : hashtags) {
			dataMap.put(hashtag, data);
			dataVersionMap.put(hashtag, newVersion);
		}
		
		serverVectors.put(server, vector);
	}
}

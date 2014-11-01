import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;
import org.junit.Test;


public class TestFEServer {
	private static boolean serverStarted = false;
	
	@Test
	public void testGetOnNoValue() {
		
		if (!serverStarted)
			startServer();
		
		String lineText = "";
		int PORT = 2350;
		String testText = "{\"q\":\"notinthere\",\"tweets\":[]}";

		String requestheaders = "GET /tweets?q=notinthere HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT);
		
		assertEquals(testText, lineText);
	}

	@Test
	public void testPost() {
		String lineText = "";
		String testText = "HTTP/1.1 201 Created";
		String tweet = "This is a #test";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		lineText = postRequest(tweet, PORT, false);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testPost2() {
		String lineText = "";
		String testText = "HTTP/1.1 201 Created";
		String tweet = "This is another #test";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		lineText = postRequest(tweet, PORT, false);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testPost3() {
		String lineText = "";
		String testText = "HTTP/1.1 201 Created";
		String tweet = "This is the #third #test";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		lineText = postRequest(tweet, PORT, false);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testPost4() {
		String lineText = "";
		String testText = "HTTP/1.1 201 Created";
		String tweet = "This is the #one that does not have a #hashtag on test";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		lineText = postRequest(tweet, PORT, false);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testGetonValue() {
		String lineText = "";
		String testText = "{\"q\":\"one\",\"tweets\":[\"This is the #one that does not have a #hashtag on test\"]}";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		//ExecutorService executor = Executors.newFixedThreadPool(3);
		String requestheaders = "GET /tweets?q=one HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT);
		
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testGet2() {
		String lineText = "";
		String testText = "{\"q\":\"one\",\"tweets\":[\"This is the #one that does not have a #hashtag on test\"]}";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		//ExecutorService executor = Executors.newFixedThreadPool(3);
		String requestheaders = "GET /tweets?q=one HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT);
		
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testBadGet() {
		String lineText = "";
		String testText = "HTTP/1.1 400 Bad Request";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		//ExecutorService executor = Executors.newFixedThreadPool(3);
		String requestheaders = "GET /tweets? HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT).trim();
		
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testBadGetURI() {
		String lineText = "";
		String testText = "HTTP/1.1 404 Not Found";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		//ExecutorService executor = Executors.newFixedThreadPool(3);
		String requestheaders = "GET /tweet?q=test HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT).trim();
		
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testBadPost() {
		String lineText = "";
		String testText = "HTTP/1.1 400 Bad Request";
		String tweet = "This is a #test";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		lineText = postRequest(tweet, PORT, true);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testPut() {
		
		if (!serverStarted)
			startServer();
		
		String lineText = "";
		int PORT = 2350;
		String testText = "HTTP/1.1 400 Bad Request";

		String requestheaders = "PUT /tweets?q=notinthere HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT);
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testDelete() {
		
		if (!serverStarted)
			startServer();
		
		String lineText = "";
		int PORT = 2350;
		String testText = "HTTP/1.1 400 Bad Request";

		String requestheaders = "DELETE /tweets?q=notinthere HTTP/1.1\n";
		lineText = getRequest(requestheaders, PORT);
		
		assertEquals(testText, lineText);
	}
	
	public String getRequest(String requestheaders, int PORT) {
		String lineText = "";
		Socket feSock;
		try {
			//Send request to DataServer
			feSock = new Socket("localhost", PORT);

			
			OutputStream out = feSock.getOutputStream();
			out.write(requestheaders.getBytes());
			//out.write(("\n").getBytes());

			// Receive response from DataServer
			String line;
			InputStreamReader is = new InputStreamReader(feSock.getInputStream());
			BufferedReader in = new BufferedReader(is);

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
			out.flush();
			out.close();
			in.close();
			feSock.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lineText;
	}
	
	@SuppressWarnings("unchecked")
	public String postRequest(String tweet, int PORT, boolean doBad) {
		
		String lineText = "";
		Socket feSock;
		try {
			feSock = new Socket("localhost", PORT);

			JSONObject obj2 = new JSONObject();
			if (doBad)
				obj2.put("tweet", tweet);
			else
				obj2.put("text", tweet);
			String responsebody = obj2.toJSONString();
			
			String requestheaders = "POST /tweets HTTP/1.1\n" + "Content-Length: "
					+ responsebody.getBytes().length + "\n\n";
			OutputStream out = feSock.getOutputStream();
			out.write(requestheaders.getBytes());
			out.write(responsebody.getBytes());
			//out.write(("\n").getBytes());

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					feSock.getInputStream()));

			while (!(line = in.readLine().trim()).equals("")) {
				lineText += line;
			}
						
			out.flush();
			out.close();
			in.close();
			feSock.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lineText;		
	}
	
	
	public void startServer() {
		int numFE = 1;
		ExecutorService executor = Executors.newFixedThreadPool(numFE + 2);
		
		try {
			executor.execute(new RunDiscServer());
			Thread.sleep(1000);
			executor.execute(new RunDataServer());
			Thread.sleep(1000);
			
			if (numFE > 0) {
				String PORT = "235";
				for(int i = 0; i < numFE; i++) {
					executor.execute(new RunFEServer(PORT+Integer.toString(i)));
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		serverStarted = true;
	}
}


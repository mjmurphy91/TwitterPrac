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


public class TestFeServerMulti {
	private static boolean serverStarted = false;
	
	@Test
	public void testGetOnNoValue() {
		
		if (!serverStarted)
			startServer();
		
		String lineText = "";
		int PORT = 2350;
		String testText = "{\"q\":\"hashtag\",\"tweets\":[]}";

		String requestheaders = "GET /tweets?q=hashtag HTTP/1.1\n";
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

		lineText = postRequest(tweet, PORT);
		
		assertEquals(testText, lineText);
		
	}
	
	@Test
	public void testGetonValue() {
		String lineText = "";
		String testText = "{\"q\":\"test\",\"tweets\":[\"This is a #test\"]}";
		int PORT = 2350;
		
		if (!serverStarted)
			startServer();

		//ExecutorService executor = Executors.newFixedThreadPool(3);
		String requestheaders = "GET /tweets?q=test HTTP/1.1\n";
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
			out.write(("\n").getBytes());

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					feSock.getInputStream()));

			while (!(line = in.readLine().trim()).equals("")) {
				if(line.equalsIgnoreCase("HTTP/1.1 200 OK")) {
					line = in.readLine();
					line = in.readLine();
					lineText = in.readLine();
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
	public String postRequest(String tweet, int PORT) {
		
		String lineText = "";
		Socket feSock;
		try {
			feSock = new Socket("localhost", PORT);

			JSONObject obj2 = new JSONObject();
			obj2.put("text", tweet);
			String responsebody = obj2.toJSONString();
			String requestheaders = "POST /tweets HTTP/1.1\n" + "Content-Length: "
					+ responsebody.getBytes().length + "\n\n";
			OutputStream out = feSock.getOutputStream();
			out.write(requestheaders.getBytes());
			out.write(responsebody.getBytes());
			out.write(("\n").getBytes());

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
		System.out.println("Starting Server");
		ExecutorService executor = Executors.newFixedThreadPool(3);
		
		try {
			executor.execute(new RunDataServer());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		serverStarted = true;
	}
}


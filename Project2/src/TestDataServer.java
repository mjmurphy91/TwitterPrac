import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;
import org.junit.Test;

public class TestDataServer {
	private static boolean serverStarted = false;
	
	@Test
	public void testGetOnNoValue() {
		
		if (!serverStarted)
			startServer();
		
		String lineText = "";
		String testText = "{\"v\":0,\"q\":\"hashtag\",\"tweets\":[]}";

		Socket dataSock;
		try {
			//Send request to DataServer
			dataSock = new Socket("localhost", 2345);

			String requestheaders = "GET /tweets?q=hashtag&v=1 HTTP/1.1\n";
			OutputStream out = dataSock.getOutputStream();
			out.write(requestheaders.getBytes());
			out.write(("\n").getBytes());
			//out.flush();
			//out.close();

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					dataSock.getInputStream()));

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
			dataSock.close();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals(testText, lineText);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPost() {
		String lineText = "";
		String testText = "HTTP/1.1 201 Created";
		
		if (!serverStarted)
			startServer();

		Socket dataSock;
		try {
			dataSock = new Socket("localhost", 2345);

			String tweet = "This is a #test";
			ArrayList<String> hashtags = new ArrayList<String>();
			hashtags.add("test");

			JSONObject obj2 = new JSONObject();
			obj2.put("tweet", tweet);
			obj2.put("hashtags", hashtags);
			String responsebody = obj2.toJSONString();
			String requestheaders = "POST /tweets HTTP/1.1\n" + "Content-Length: "
					+ responsebody.getBytes().length + "\n\n";
			OutputStream out = dataSock.getOutputStream();
			out.write(requestheaders.getBytes());
			out.write(responsebody.getBytes());
			out.write(("\n").getBytes());

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					dataSock.getInputStream()));

			while (!(line = in.readLine().trim()).equals("")) {
				lineText += line;
			}
						
			out.flush();
			out.close();
			in.close();
			dataSock.close();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals(testText, lineText);
	}
	
	@Test
	public void testGetonValue() {
		String lineText = "";
		String testText = "{\"v\":1,\"q\":\"test\",\"tweets\":[\"This is a #test\"]}";
		
		if (!serverStarted)
			startServer();

		Socket dataSock;
		try {
			//Send request to DataServer
			dataSock = new Socket("localhost", 2345);

			String requestheaders = "GET /tweets?q=test&v=0 HTTP/1.1\n";
			OutputStream out = dataSock.getOutputStream();
			out.write(requestheaders.getBytes());
			out.write(("\n").getBytes());

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					dataSock.getInputStream()));

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
			dataSock.close();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals(testText, lineText);
	}
	
	public void startServer() {
		System.out.println("Starting Server");
		ExecutorService executor = Executors.newFixedThreadPool(1);
		
		try {
			executor.execute(new RunDataServer());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		serverStarted = true;
	}
}
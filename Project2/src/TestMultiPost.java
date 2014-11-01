import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.json.simple.JSONObject;


public class TestMultiPost implements Runnable{

	private String tweet;
	private String testText;
	
	public TestMultiPost(String tweet, String tt) {
		this.tweet = tweet;
		testText = tt;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		String lineText = "";
		Socket feSock;
		try {
			feSock = new Socket("localhost", 2350);

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
		
		assertEquals(testText, lineText);
	}

}

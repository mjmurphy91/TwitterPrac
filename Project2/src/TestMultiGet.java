import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class TestMultiGet implements Runnable {
	
	private String requestheaders;
	private String testText;
	private int PORT;
	
	public TestMultiGet(String rh, String tt, int pn) {
		requestheaders = rh;
		testText = tt;
		PORT = pn;
	}

	@Override
	public void run() {
		
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
		
		assertEquals(testText, lineText);
	}

}

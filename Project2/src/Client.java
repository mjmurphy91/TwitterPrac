import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.json.simple.JSONObject;


public class Client {
	
	/**
	 * Main method which prompts user for input
	 */
	@SuppressWarnings("resource")
	public static void main(String args[]) {
		if(args.length < 2) {
			System.out.println("Usage: java Client IP PORT");
			System.exit(0);
		}
		String IP = args[0];
		int PORT = Integer.parseInt(args[1]);
		Scanner scan = new Scanner(System.in);
		System.out.println("Please choose operation:" 
				+ "\nInsert q to quit"
				+ "\n(GET and POST currently implemented)");
		String input = scan.next();
		while(!input.equalsIgnoreCase("q")) {
			String nextInput = "";
			if (input.equalsIgnoreCase("GET")) {
				System.out.println("Please enter hashtag to search for:");
				nextInput = scan.next();
				String requestheaders = "GET /tweets?q="+ nextInput +" HTTP/1.1\n";
				String results = getRequest(requestheaders, IP, PORT);
				System.out.println("Results:\n" + results);
			}
			else if (input.equalsIgnoreCase("POST")) {
				System.out.println("Please enter tweet to post:");
				scan.nextLine();
				nextInput = scan.nextLine();
				String request = nextInput;
				String results = postRequest(request, IP, PORT);
				System.out.println("Results:\n" + results);
			}
			else {
				System.out.println("Operation: " + input 
						+ " has not been implemented");
			}
			
			System.out.println("Please choose next operation:");
			input = scan.next();
		}
		
	}
	
	/**
	 * Generalized method to set up a post request given tweet to post and port
	 * number of FEServer
	 */
	@SuppressWarnings("unchecked")
	public static String postRequest(String tweet, String IP, int PORT) {
		
		String lineText = "";
		Socket feSock;
		try {
			feSock = new Socket(IP, PORT);

			JSONObject obj2 = new JSONObject();
			obj2.put("text", tweet);
			String responsebody = obj2.toJSONString();
			String requestheaders = "POST /tweets HTTP/1.1\n" 
					+ "Content-Length: "
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
	
	/**
	 * Generalized method to set up a get request given a requestheader and 
	 * port number of FEServer
	 */
	public static String getRequest(String requestheaders, String IP, int PORT) {
		String lineText = "";
		Socket feSock;
		try {
			//Send request to DataServer
			feSock = new Socket(IP, PORT);

			
			OutputStream out = feSock.getOutputStream();
			out.write(requestheaders.getBytes());

			// Receive response from DataServer
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(
					feSock.getInputStream()));

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

}

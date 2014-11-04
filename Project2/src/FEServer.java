import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class FEServer {
	
	@SuppressWarnings("resource")
	public static void main (String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Usage: java FEServer PORT configFile");
			System.exit(0);
		}

		int PORT = Integer.parseInt(args[0]);


		System.out.println("Starting FE Server");
		String[] fullIP = InetAddress.getLocalHost().toString().split("/");
		String IP = fullIP[fullIP.length - 1];
		System.out.println("IP: " + IP);
		System.out.println("PORT: " + PORT + "\n");
		ExecutorService executor = Executors.newFixedThreadPool(10);
		ServerSocket serversock = new ServerSocket(PORT);
		Socket sock;
		String discServer = getDiscServer(args[1]);
		
		String[] dataServer = new String[1];
		dataServer[0] = getInitialServer(discServer);
		DataStore cache = new DataStore(dataServer[0]);
		
		while (true) {
			sock = new Socket();
			sock = serversock.accept();
			sock.setSoTimeout(10000);
			executor.execute(new FEReqProc(sock, cache, dataServer, discServer));
		}
	}

	private static String getDiscServer(String file) {
		String server = "";
		Path path = FileSystems.getDefault().getPath(".", file);
		try {
			server = Files.readAllLines(path, Charset.defaultCharset()).get(0);
		} catch (IOException e) {
			System.out.println("Could not read file: " + file);
		}
		return server;
	}
	
	private static String getInitialServer(String discServer) {
		String[] discParts = discServer.split(":");
		String lineText = "";
		Socket discSock;
		try {
			//Send request to DataServer
			String IP = discParts[0];
			int PORT = Integer.parseInt(discParts[1]);
			discSock = new Socket(IP, PORT);
			String requestheaders = "GET /tweets HTTP/1.1\n";

			OutputStream out = discSock.getOutputStream();
			out.write(requestheaders.getBytes());

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
			out.flush();
			out.close();
			in.close();
			discSock.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String server = "";
		JSONParser parser = new JSONParser();
		try {
			JSONObject obj=(JSONObject) parser.parse(lineText);
			server = (String) obj.get("min");
		} catch (ParseException e) {
			System.out.println("Could not parse lineText");
		}
		
		if(server.split(":").length < 2) {
			return "Bad:1";
		}
		
		return server;
	}
}
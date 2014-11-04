import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class SeededFEServer {

	@SuppressWarnings("resource")
	public static void main (String[] args) throws Exception {
		if(args.length < 4) {
			System.out.println("Usage: java SeededFEServer PORT DataIP DataPORT SeedIP");
			System.exit(0);
		}

		int PORT = Integer.parseInt(args[0]);
		
		System.out.println("Starting Seeded FE Server");
		String[] fullIP = InetAddress.getLocalHost().toString().split("/");
		String IP = fullIP[fullIP.length - 1];
		System.out.println("IP: " + IP);
		System.out.println("PORT: " + PORT + "\n");
		ExecutorService executor = Executors.newFixedThreadPool(10);
		ServerSocket serversock = new ServerSocket(PORT);
		Socket sock;
		
		String[] dataServer = new String[1];
		dataServer[0] = args[1] + ":" + args[2];
		
		DataStore cache = new DataStore(dataServer[0]);
		String server = args[3];
		int vector = 100;
		ArrayList<String> hashtags = new ArrayList<String>();
		hashtags.add("test");
		String tweet = "Higher vector clock #test";
		int newVersion = 1;
		cache.seed(server, vector, hashtags, tweet, newVersion);
		
		while (true) {
			sock = new Socket();
			sock = serversock.accept();
			sock.setSoTimeout(10000);
			executor.execute(new FEReqProc(sock, cache, dataServer, null));
		}
	}
}

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DiscServer {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		
		if(args.length < 1) {
			System.out.println("Usage: java DiscServer PORT");
			System.exit(0);
		}
		
		int PORT = Integer.parseInt(args[0]);
		System.out.println("Starting Discovery Server");
		String[] fullIP = InetAddress.getLocalHost().toString().split("/");
		String IP = fullIP[fullIP.length - 1];
		System.out.println("IP: " + IP);
		System.out.println("PORT: " + PORT + "\n");

		DataStore ds = new DataStore();
		ExecutorService executor = Executors.newFixedThreadPool(10);
		ServerSocket serversock = new ServerSocket(PORT);
		Socket sock;
		
		while (true) {
			sock = new Socket();
			sock = serversock.accept();
			sock.setSoTimeout(10000);
			executor.execute(new DiscReqProc(sock, ds));
		}
	}

}

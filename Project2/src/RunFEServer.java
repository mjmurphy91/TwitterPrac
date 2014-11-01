
public class RunFEServer implements Runnable {
	
	String PORT;
	
	public RunFEServer(String PORT) {
		this.PORT = PORT;
	}
	
	public void run() {
		String[] args = new String[3];
		args[0] = PORT;
		args[1] = "config1";

		try {
			FEServer.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


public class RunDataServer implements Runnable {
	
	public void run() {
		String[] args = new String[2];
		args[0] = "2345";
		args[1] = "config1";
		
		try {
			DataServer.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
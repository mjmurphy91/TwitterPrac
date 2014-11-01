public class RunDiscServer implements Runnable {
		
	public void run() {
		String[] args = new String[1];
		args[0] = "2300";
		
		try {
			DiscServer.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
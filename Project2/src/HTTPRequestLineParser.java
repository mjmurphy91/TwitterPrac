import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class HTTPRequestLineParser {

	/**
	 * This method takes as input the Request-Line exactly as it is read from the socket.
	 * It returns a Java object of type HTTPRequestLine containing a Java representation of
	 * the line.
	 *
	 * The signature of this method may be modified to throw exceptions you feel are appropriate.
	 * The parameters and return type may not be modified.
	 *
	 * 
	 * @param line
	 * @return
	 */
	public static HTTPRequestLine parse(String line) {
	    //A Request-Line is a METHOD followed by SPACE followed by URI followed
		//by SPACE followed by VERSION
		if (line == null) {
			return null;
		}
		
		String[] splitLine = line.split(" ");
		if (splitLine.length != 3)
			return null;

		HTTPRequestLine request = new HTTPRequestLine();
		
		try {
			request.setMethod(splitLine[0]);
		} catch (IllegalArgumentException e) {
			return null;
		}
		
		//A VERSION is 'HTTP/' followed by 1.0 or 1.1
		String[] versionCheck = splitLine[2].split("/");
		if (versionCheck.length < 2
				|| !versionCheck[0].equals("HTTP")
				|| (!versionCheck[1].equals("1.0") 
						&& !versionCheck[1].equals("1.1")))
			return null;
		request.setVersion(versionCheck[1]);
		
		//A URI is a '/' followed by PATH followed by optional '?' PARAMS
		int index = splitLine[1].indexOf("?");
		String uri = splitLine[1];
		String parameters = "";
		
		if (index >= 0) {
			uri = splitLine[1].substring(0, index);
			parameters = splitLine[1].substring(index+1);
		}
		
		if(parameters.length() > 1) {
			//PARAMS are of the form key'='value'&'
			try {
				String[] paramCheck = URLDecoder.decode(parameters, "UTF-8")
						.split("\\&");
				for (String param : paramCheck) {
					String[] keyVal = param.split("=");
					if (keyVal.length > 1)
						request.setParameters(keyVal[0], keyVal[1]);
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}			
		}
		
		if(!uri.startsWith("/")) {
			return null;
		}
		request.setURI(uri.substring(1));

		return request;
	}
		
}

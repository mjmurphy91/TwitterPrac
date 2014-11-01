import java.util.HashMap;

/**
HTTPRequestLine is a data structure that stores a Java representation of the parsed Request-Line.
 **/
public class HTTPRequestLine {

	private HTTPConstants.HTTPMethod method;
	private String uripath;
	private HashMap<String, String> parameters;
	private String httpversion;

	public HTTPRequestLine() {
		method = null;
		uripath = null;
		parameters = new HashMap<String, String>();
		httpversion = null;
	}
	
	public String getMethod() {
		if (method == null)
			return null;
		return method.toString();
	}
	
	public String getURI() {
		return uripath;
	}
	
	public HashMap<String, String> getParameters() {
		return parameters;
	}
	
	public String getVersion() {
		return httpversion;
	}
	
	@SuppressWarnings("static-access") 
	public void setMethod(String m) throws IllegalArgumentException {
			method = method.valueOf(m);
	}
	
	public void setURI(String uri) {
		uripath = uri;
	}
	
	public void setParameters(String key, String value) {
		parameters.put(key, value);
	}
	
	public void setVersion(String ver) {
		httpversion = ver;
	}
}

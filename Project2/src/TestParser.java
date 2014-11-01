import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestParser {

	@Test
	public void baseTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?city=seattle HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setParameters("city", "seattle");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}
	
	@Test
	public void multiParamTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?city=seattle&state=washington&country=united+states HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setParameters("city", "seattle");
		test.setParameters("state", "washington");
		test.setParameters("country", "united states");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}

	@Test
	public void emptyTest() {
		HTTPRequestLine result = HTTPRequestLineParser.parse("");

		HTTPRequestLine test = null;

		assertEquals(test, result);
	}

	@Test
	public void nullTest() {
		HTTPRequestLine result = HTTPRequestLineParser.parse(null);

		HTTPRequestLine test = null;

		assertEquals(test, result);
	}

	@Test
	public void badMethodTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GETT /test?city=seattle HTTP/1.1");

		HTTPRequestLine test = null;

		assertEquals(test, result);
	}

	@Test
	public void badVersionTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?city=seattle HTTTP/1.1");

		HTTPRequestLine test = null;

		assertEquals(test, result);
	}

	@Test
	public void badVersionTest2() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?city=seattle HTTP/1.2");

		HTTPRequestLine test = null;

		assertEquals(test, result);
	}

	@Test
	public void extraQuestionMarkTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?city=seattle? HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setParameters("city", "seattle?");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}

	@Test
	public void noParametersTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}

	@Test
	public void noParametersButAQuestionTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test? HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}

	@Test
	public void noValueForKeyTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET /test?cityseattle HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setURI("test");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}
	
	@Test
	public void justASlashTest() {
		HTTPRequestLine result = HTTPRequestLineParser
				.parse("GET / HTTP/1.1");

		HTTPRequestLine test = new HTTPRequestLine();
		test.setMethod("GET");
		test.setURI("");
		test.setVersion("1.1");

		assertEquals(test.getMethod(), result.getMethod());
		assertEquals(test.getURI(), result.getURI());
		assertEquals(test.getParameters(), result.getParameters());
		assertEquals(test.getVersion(), result.getVersion());
	}

}
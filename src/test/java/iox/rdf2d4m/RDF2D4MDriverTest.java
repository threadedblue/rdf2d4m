package iox.rdf2d4m;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

public class RDF2D4MDriverTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Test
	public void testGetConfig() {
		RDF2D4MConfig sut = RDF2D4MDriver.getConfig();
		assertNotNull(sut);
		Map<String, String> map = sut.getArgs();
		assertNotNull(map);
		assertEquals(0, map.size() % 2);
	}

	@Test
	public void testMerge() {
		RDF2D4MConfig app = RDF2D4MDriver.getConfig();
		assertNotNull(app);
		String[] args = {"-i", "xxx", "-o", "yyy"};
		Map<String, String> cfg = app.getArgs();
		assertEquals("ec2", cfg.get("o"));
		RDF2D4MDriver.merge(args, cfg);
		assertEquals("yyy", cfg.get("o"));
		assertEquals("xxx", cfg.get("i"));
		assertNotNull(cfg.get("l"));
	}
}

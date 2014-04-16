package me.boyce.elastisearch.dd.river;

import java.util.HashMap;
import java.util.Map;

import me.boyce.elasticsearch.dd.river.pull.IRiverPuller;
import me.boyce.elasticsearch.dd.river.push.parser.DefaultDocumentParserFactory;

import org.elasticsearch.client.Client;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class SimpleDataDriverTest extends AbstractRiverNodeTest {

	private Client client;
	
	@Test
	@Parameters({"file"})
	public void testSimpleDataDriver(@Optional String file) throws Exception{
		startNode("1");
		client = client("1");
		IRiverPuller puller = new SimpleRiverPuller();
		puller.client(client);
		puller.parserFactory(new DefaultDocumentParserFactory());
		puller.riverName(INDEX);
		puller.index(INDEX);
		Map<String, Object> settings = new HashMap<>(); 
		if(file == null){
			file = "a.txt";
		}
		settings.put("file", file);
		puller.settings(settings);
		puller.checkAndCreateIndex(INDEX);
		puller.setMappings();
		puller.run();
	}
}

package me.boyce.elasticsearch.dd.river.push.parser;

import java.util.Map;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class DefaultDocumentParserFactory implements IDocumentParserFactory {

	private final static ESLogger LOG = ESLoggerFactory
			.getLogger(DefaultDocumentParserFactory.class.getName());

	private Map<String, IDocumentParser> parsers = null;

	/**
	 * 默认文档解析器
	 */
	private IDocumentParser dftDocParser = new DefaultDocumentParser();

	public DefaultDocumentParserFactory() {
	}

	public DefaultDocumentParserFactory(Map<String, IDocumentParser> parsers) {
		this.parsers = parsers;
	}

	public Map<String, IDocumentParser> getParsers() {
		return parsers;
	}

	public void setParsers(Map<String, IDocumentParser> parsers) {
		this.parsers = parsers;
	}

	@Override
	public IDocumentParser getParser(String dt) {
		if (parsers != null) {
			IDocumentParser parser = parsers.get(dt);
			if (parser != null)
				return parser;
		}
		// FIXME?
		return dftDocParser;
	}

	public JSONObject parse(String doc) {
		try {
			JSONObject jo = JSON.parseObject(doc);
			String dataType = jo.getString(DataDriverConstants.FIELD_DATATYPE);
			IDocumentParser parser = getParser(dataType);
			return parser.parseDocument(doc);
		} catch (Exception e) {
			LOG.warn("error when parse document: {}", doc);
		}
		return null;
	}
}

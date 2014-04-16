package me.boyce.elasticsearch.dd.river.push.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.script.ScriptService;

public abstract class AbstractRiverConsumer implements IRiverConsumer {
	public static final ESLogger LOG = ESLoggerFactory.getLogger(AbstractRiverConsumer.class
			.getName());

	protected String riverName;
	/**
	 * 设置可接受数据类型
	 */
	protected Set<String> acceptableTypes = null;
	protected String index = null;

	protected Client client;
	protected IDocumentParserFactory parserFactory;
	protected ScriptService scriptService;
	protected Map<String, Object> consumerSettings;
	protected Map<String, Object> typeMappings;
	protected Map<String, Object> indexSetting;

	@Override
	public IRiverConsumer client(Client client) {
		this.client = client;
		return this;
	}

	@Override
	public IRiverConsumer riverName(String riverName) {
		this.riverName = riverName;
		return this;
	}

	@Override
	public IRiverConsumer settings(Map<String, Object> settings) {
		this.consumerSettings = settings;
		return this;
	}

	@Override
	public IRiverConsumer mappings(Map<String, Object> mappings) throws Exception {
		this.typeMappings = mappings;
		return this;
	}
	
	@Override
	public IRiverConsumer indexSetting(Map<String, Object> indexSetting) throws Exception {
		this.indexSetting = indexSetting;
		return this;
	}

	/**
	 * 设置mapping
	 */
	public void setMappings() {
		if (typeMappings != null) {
			for (String key : typeMappings.keySet()) {
				PutMappingRequest putMappingRequest = new PutMappingRequest(index);
				putMappingRequest.type(key);
				Map<String, Object> singleMap = new HashMap<>();
				singleMap.put(key, typeMappings.get(key));
				putMappingRequest.source(singleMap);
				// 默认覆盖设置
				putMappingRequest.ignoreConflicts(true);
				client.admin().indices().putMapping(putMappingRequest).actionGet();
			}
		}
	}

	@Override
	public IRiverConsumer parserFactory(IDocumentParserFactory factory) {
		this.parserFactory = factory;
		return this;
	}

	@Override
	public IRiverConsumer scriptService(ScriptService scriptService) {
		this.scriptService = scriptService;
		return this;
	}

	@Override
	public IRiverConsumer acceptableTypes(Set<String> types) {
		this.acceptableTypes = types;
		return this;
	}

	@Override
	public IRiverConsumer index(String index) {
		this.index = index;
		return this;
	}

	/**
	 * 检查并创建索引
	 */
	public void checkAndCreateIndex(String index) {
		LOG.debug("check and create index: {}", index);

		if (!client.admin().indices().prepareExists(index).execute()
				.actionGet().isExists()) {
			CreateIndexRequestBuilder builder = client.admin().indices()
					.prepareCreate(index);
			if (indexSetting != null) {
				builder.setSettings(indexSetting);
			}
			builder.execute().actionGet();
		}
	}

	@Override
	public void run() {
		doStart();
	}

	@Override
	public void stop() {
		// do something
		doStop();
	}

	/**
	 * 由子类继承实现具体的执行操作
	 */
	protected abstract void doStart();

	/**
	 * 由子类继承实现具体的执行操作
	 */
	protected abstract void doStop();

}

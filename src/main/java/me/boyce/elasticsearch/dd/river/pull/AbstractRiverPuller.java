package me.boyce.elasticsearch.dd.river.pull;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;
import me.boyce.elasticsearch.dd.hash.Hashing;
import me.boyce.elasticsearch.dd.hash.MurmurHash;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParser;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParserFactory;
import me.boyce.elasticsearch.dd.util.IndexUtil;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.script.ScriptService;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 抽象的IRiverPuller实现，提供一些公共的基础属性与方法，为子类提供状态数据的存储和获取功能（默认存储与river所在的索引下）。
 *
 * @author boyce
 * @created: 2013-6-24 下午3:56:40
 * @version 0.1
 *
 */
public abstract class AbstractRiverPuller implements IRiverPuller {

	public static final ESLogger LOG = ESLoggerFactory
			.getLogger(AbstractRiverPuller.class.getName());

	protected final static Hashing HASHER = new MurmurHash();

	protected Client client = null;

	protected String index = null;

	protected BulkProcessor bulkProcessor;
	protected BulkRequest bulkRequest;
	protected IDocumentParserFactory parserFactory;
	protected ScriptService scriptService;
	/**
	 * 拉取相关设置
	 */
	protected Map<String, Object> pullerSettings = null;
	/**
	 * mapping设置
	 */
	protected Map<String, Object> typeMappings = null;
	protected Map<String, Object> indexSetting = null;
	/**
	 * 设置可接受数据类型
	 */
	protected Set<String> acceptableTypes = null;

	/**
	 * 状态信息存储索引路径
	 */
	protected String statusIndex = "_river";
	/**
	 * 状态信息存储索引类型
	 */
	protected String statusType;
	/**
	 * 状态信息存储索引ID
	 */
	protected String statusId = "_pull_status";

	protected SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	protected int maxBulkActions = 1000;
	protected int maxConcurrentBulkRequests = 30;
	private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

		@Override
		public void beforeBulk(long executionId, BulkRequest request) {
			preProcessBeforeBulk(executionId, request.numberOfActions());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse response) {
			numberOfActions4Bulk(executionId, request.numberOfActions());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				Throwable failure) {
		}
	};

	/**
	 * 子类获取每次提交索引的数量
	 *
	 * @param num
	 */
	protected void numberOfActions4Bulk(long executionId, int num) {
	}

	/**
	 * 批量提交索引前的预处理
	 */
	protected void preProcessBeforeBulk(long executionId, int num) {
	}

	@Override
	public IRiverPuller parserFactory(IDocumentParserFactory factory) {
		this.parserFactory = factory;
		return this;
	}

	@Override
	public IRiverPuller scriptService(ScriptService scriptService) {
		this.scriptService = scriptService;
		return this;
	}

	@Override
	public IRiverPuller riverName(String riverName) {
		this.statusType = riverName;
		if (isAsync()) {
			this.bulkProcessor = BulkProcessor.builder(client, listener)
					.setBulkActions(maxBulkActions - 1)
					.setConcurrentRequests(maxConcurrentBulkRequests)
					.setFlushInterval(TimeValue.timeValueSeconds(30)).build();
		} else {
			this.bulkRequest = new BulkRequest();
		}
		return this;
	}

	@Override
	public IRiverPuller client(Client client) {
		this.client = client;
		return this;
	}

	@Override
	public IRiverPuller settings(Map<String, Object> settings) throws Exception {
		this.pullerSettings = settings;
		if (settings == null) {
			throw new IllegalArgumentException("missing puller settings");
		}
		return this;
	}

	@Override
	public IRiverPuller index(String idx) {
		this.index = idx;
		return this;
	}

	@Override
	public IRiverPuller mappings(Map<String, Object> mappings) throws Exception {
		this.typeMappings = mappings;
		return this;
	}

	@Override
	public IRiverPuller indexSetting(Map<String, Object> indexSetting)
			throws Exception {
		this.indexSetting = indexSetting;
		return this;
	}

	/**
	 * 是否使用异步策略。子类覆盖该方法设置同步异步策略
	 *
	 * @return
	 */
	protected boolean isAsync() {
		return true;
	}

	public String getStatusIndex() {
		return statusIndex;
	}

	public String getStatusType() {
		return statusType;
	}

	@Override
	public IRiverPuller acceptableTypes(Set<String> types) {
		this.acceptableTypes = types;
		return this;
	}

	/**
	 * 存储状态ID
	 *
	 * @return
	 */
	protected String getStatusId() {
		return statusId;
	}

	/**
	 * 从ES集群获取上次存储状态
	 *
	 * @return
	 */
	protected Map<String, Object> readStatus() {
		GetRequest getRequest = new GetRequest(getStatusIndex(),
				getStatusType(), getStatusId());
		GetResponse getResponse = null;
		try {
			getResponse = client.get(getRequest).actionGet();
			Map<String, Object> respMap = getResponse.getSourceAsMap();
			if (respMap == null) {
				return null;
			}
			LOG.info("readStatus::getResponse : {}", respMap);
			return respMap;
		} catch (IndexMissingException e) {
			// ignore
		} catch (Exception e) {
			LOG.error("error when get index data: {}/{}/{}", e,
					getStatusIndex(), getStatusType(), getStatusId());
		}
		return new HashMap<String, Object>();
	}

	/**
	 * 将状态信息存储至ES集群
	 *
	 * @param statusMap
	 */
	protected void saveStatus(Map<String, Object> statusMap) {
		IndexRequest indexRequest = new IndexRequest(getStatusIndex(),
				getStatusType(), getStatusId());
		indexRequest.source(statusMap);
		client.index(indexRequest).actionGet();
	}

	/**
	 * 将状态信息存储至ES集群
	 *
	 * @param statusMap
	 */
	protected void saveStatus(JSONObject statusJo) {
		IndexRequest indexRequest = new IndexRequest(getStatusIndex(),
				getStatusType(), getStatusId());
		indexRequest.source(statusJo.toJSONString());
		client.index(indexRequest).actionGet();
	}

	public static boolean isBlank(String str) {
		int strLen;
		if (str == null || (strLen = str.length()) == 0) {
			return true;
		}
		for (int i = 0; i < strLen; i++) {
			if ((Character.isWhitespace(str.charAt(i)) == false)) {
				return false;
			}
		}
		return true;
	}

	// ****************************************************//
	// ****************************************************//
	// ****************************************************//
	/**
	 * 将毫秒转化为字符串
	 *
	 * @param ms
	 *            毫秒
	 * @return
	 */
	public static String ms2Str(long ms) {

		long day = ms / (24 * 60 * 60 * 1000);
		long hour = (ms / (60 * 60 * 1000) - day * 24);
		long min = ((ms / (60 * 1000)) - day * 24 * 60 - hour * 60);
		long sec = (ms / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
		long msec = ms % 1000;

		String tStr = "";
		if (day > 0) {
			tStr += day + " days.";
		}
		if (hour > 0) {
			tStr += hour + " hours.";
		}
		if (min > 0) {
			tStr += min + " minutes.";
		}
		if (sec > 0) {
			tStr += sec + " seconds.";
		}
		if (msec > 0) {
			tStr += msec + " milliseconds.";
		}

		return tStr;
	}

	/**
	 * 确保删除索引，因为其是异步操作
	 */
	public void ensureDelAction(String ix2del) {
		int wcount = 0;
		try {
			while (client.admin().indices().prepareExists(ix2del).execute()
					.actionGet().isExists()) {
				TimeUnit.SECONDS.sleep(3);
				wcount++;
				if (wcount == 20) {
					LOG.error("~~~~~~~~~~~ can't delete " + ix2del
							+ ", delete it again.");
					DeleteIndexRequest deleteRequest = new DeleteIndexRequest(
							ix2del);
					client.admin().indices().delete(deleteRequest);
					TimeUnit.SECONDS.sleep(6);
					break;
				}
			}
			LOG.info(index + " :: ~~~~~~~~~~~ {} has deleted.wcount == {}",
					ix2del, wcount);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(index
					+ " :: ~~~~~~~~!!!!!!!! error happened in deleting "
					+ ix2del + ". wcount == {}", e, wcount);
		}
	}

	public List<String> getAllIndexByAliasExceptCurIx(Client client,
			String aliasName, String newIndexName) throws Exception {
		ClusterStateRequest clusterStateRequest = Requests
				.clusterStateRequest().filterRoutingTable(true)
				.filterNodes(true).filteredIndices(aliasName + "*");
		clusterStateRequest.listenerThreaded(false);
		ActionFuture<ClusterStateResponse> future = client.admin().cluster()
				.state(clusterStateRequest);
		ClusterStateResponse response = future.get(5, TimeUnit.SECONDS);
		List<String> idxNames = new ArrayList<String>();
		try {
			MetaData metaData = response.getState().metaData();
			for (IndexMetaData indexMetaData : metaData) {
				String tIx = indexMetaData.index();
				if (!tIx.equalsIgnoreCase(newIndexName)) {
					LOG.info(
							"======= index to delete : {}, current alias : {}",
							tIx, aliasName);
					idxNames.add(tIx);
				}
			}
		} catch (Throwable e) {
			LOG.error("error when fetch alias for index ", e);
		}
		return idxNames;
	}

	public List<String> getIndexByAlias2Array(Client client, String alias)
			throws Exception {
		// 先判断别名是否存在
		if (!client.admin().indices().prepareExists(alias).execute()
				.actionGet().isExists()) {
			// 如果别名不存在，则说明相应的索引也不存在
			return null;
		}
		List<String> idxNames = new ArrayList<String>();
		ClusterStateRequest clusterStateRequest = Requests
				.clusterStateRequest().filterRoutingTable(true)
				.filterNodes(true).filteredIndices(alias);
		clusterStateRequest.listenerThreaded(false);
		ActionFuture<ClusterStateResponse> future = client.admin().cluster()
				.state(clusterStateRequest);
		ClusterStateResponse response = future.get(5, TimeUnit.SECONDS);
		try {
			MetaData metaData = response.getState().metaData();
			for (IndexMetaData indexMetaData : metaData) {
				idxNames.add(indexMetaData.index());
			}
		} catch (Throwable e) {
			LOG.error("error when fetch alias for index ", e,
					idxNames.toString());
		}
		return idxNames;
	}

	/**
	 * 通过别名生成索引名称
	 *
	 * @param alias
	 * @return
	 */
	public String genIndexName(String alias) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		String dtStr = sdf.format(new Date());
		return String.format("%s_%s", alias, dtStr);
	}

	public static String getValue(Map<String, Object> map, String key,
			String defaultValue) {
		Object value = map.get(key);
		if (value == null)
			value = defaultValue;
		return (String) value;
	}

	/**
	 * 更新拉取状态
	 */
	public void saveNSStatus(String status, String ixName) {
		JSONObject jo = new JSONObject();
		jo.put("status", status);
		jo.put("indexName", ixName);
		String dtStr = sdf.format(new Date());
		jo.put("time", dtStr);
		saveStatus(jo);
		LOG.info("data new status : \r\n" + jo);
	}

	// ****************************************************//
	// ****************************************************//
	// ****************************************************//

	/**
	 * 取出原始字段数据，元数据去除。
	 * <p>
	 * 子类可以重新实现当前方法，对具体数据进行操作，譬如，把文件url转换为文件内容等
	 *
	 * @param doc
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected JSONObject unwrapDoc(JSONObject doc, String type) {
		if (doc == null || isBlank(type)) {
			return null;
		}

		// 解析器存在则使用解析器进行解析
		IDocumentParser parser = parserFactory.getParser(type);
		if (parser != null) {
			try {
				return parser.parseDocument(doc);
			} catch (Exception e) {
				LOG.error("error when parse document: {}", e, doc);
			}
			return new JSONObject();
		} else {
			Map<String, Object> typeMapping = (Map<String, Object>) typeMappings
					.get(type);
			Map<String, Object> propertiesMap = null;
			if (typeMapping != null) {
				propertiesMap = (Map<String, Object>) typeMapping
						.get("properties");
			}
			JSONObject body = new JSONObject();
			for (String key : doc.keySet()) {
				if (key.equals(DataDriverConstants.FIELD_DATASRC)
						|| key.equals(DataDriverConstants.FIELD_DATATYPE)
						|| key.equals(DataDriverConstants.FIELD_OP_TYPE)
						|| key.equals(DataDriverConstants.FIELD_ID)) {
					continue;
				}
				// 仅获取mapping指定的字段
				if (propertiesMap != null) {
					if (propertiesMap.get(key) != null) {
						body.put(key, doc.get(key));
					}
				} else {
					body.put(key, doc.get(key));
				}
			}
			return body;
		}
	}

	/**
	 * 根据索引，类型，ID和文档原始数据生成索引请求
	 *
	 * @param index
	 * @param type
	 * @param id
	 * @param doc
	 * @return
	 */
	protected IndexRequest genIndexRequest(String index, String type,
			String id, JSONObject doc) {
		JSONObject result = unwrapDoc(doc, type);
		if (result != null) {
			IndexRequest indexRequest = new IndexRequest(index, type,
					IndexUtil.genId(index, type, id));
			indexRequest.source(result);
			return indexRequest;
		}
		return null;
	}

	/**
	 * 将数据添加到索引中
	 *
	 * @param data
	 * @param type
	 */
	protected void addToIndex(JSONArray data, String type) {
		for (int i = 0; i < data.size(); i++) {
			JSONObject singleDoc = data.getJSONObject(i);
			addToIndex(singleDoc, type);
		}
	}

	/**
	 * 刚刚提交索引的数据
	 *
	 * @param jo
	 */
	protected void justIndexData(JSONObject jo) {
	}

	protected void addToIndex(JSONObject jo, String type) {
		LOG.debug("addToIndex -- jo: {} type: {}", jo, type);
		IndexRequest idxReq = genIndexRequest(index, type,
				(String) jo.get(DataDriverConstants.FIELD_ID), jo);
		if (idxReq != null) {
			justIndexData(jo);
			if (isAsync()) {
				bulkProcessor.add(idxReq);
			} else {
				bulkRequest.add(idxReq);
				if (bulkRequest.numberOfActions() >= maxBulkActions) {
					client.bulk(bulkRequest).actionGet();
					bulkRequest = new BulkRequest();
				}
			}
		}
	}

	protected void flush() {
		// 同步才需手动刷
		if (!isAsync()) {
			if (bulkRequest.numberOfActions() > 0) {
				client.bulk(bulkRequest).actionGet();
				bulkRequest = new BulkRequest();
			}
		}
	}

	/**
	 * 设置mapping
	 */
	public void setMappings() {
		if (typeMappings != null) {
			for (String key : typeMappings.keySet()) {
				PutMappingRequest putMappingRequest = new PutMappingRequest(
						index);
				putMappingRequest.type(key);
				Map<String, Object> singleMap = new HashMap<>();
				singleMap.put(key, typeMappings.get(key));
				putMappingRequest.source(singleMap);
				// 默认覆盖设置
				putMappingRequest.ignoreConflicts(true);
				client.admin().indices().putMapping(putMappingRequest)
						.actionGet();
			}
		}
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
		if (isAsync()) {
			bulkProcessor.close();
		}
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

package me.boyce.elasticsearch.dd.river.push.rabbitmq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;
import me.boyce.elasticsearch.dd.river.push.parser.AbstractRiverConsumer;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParser;
import me.boyce.elasticsearch.dd.util.IndexUtil;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ExecutableScript;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * 默认使用RabbitMQ作为推送实现
 * 
 * @author boyce
 * @created: 2013-6-24 下午1:16:19
 * @version 0.1
 * 
 */
public class RabbitMQConsumer extends AbstractRiverConsumer {

	public static final ESLogger LOG = ESLoggerFactory.getLogger(RabbitMQConsumer.class.getName());

	private volatile ConnectionFactory connectionFactory;
	private Connection connection;
	private Channel channel;

	private volatile boolean closed = false;

	private Address[] rabbitAddresses;
	private String rabbitUser;
	private String rabbitPassword;
	private String rabbitVhost;

	private String rabbitQueue;
	private boolean rabbitQueueDeclare;
	private boolean rabbitQueueBind;
	private String rabbitExchange;
	private String rabbitExchangeType;
	private String rabbitRoutingKey;
	private boolean rabbitExchangeDurable;
	private boolean rabbitExchangeDeclare;
	private boolean rabbitQueueDurable;
	private boolean rabbitQueueAutoDelete;
	private Map<String, Object> rabbitQueueArgs = null;
	private String fileDLPath;

	private int bulkSize;

	private ExecutableScript bulkScript;
	private ExecutableScript script;

	BulkProcessor bulkProcessor;

	@SuppressWarnings("unchecked")
	private void resolveSettings() {
		if (consumerSettings.containsKey("servers")) {
			String[] singleHosts = consumerSettings.get("servers").toString().split(",");
			List<Address> addresses = new ArrayList<Address>();
			for (String singleHost : singleHosts) {
				String[] strs = singleHost.split(":");
				String host = strs[0];
				int port = strs.length > 1 ? Integer.parseInt(strs[1]) : AMQP.PROTOCOL.PORT;
				addresses.add(new Address(host, port));
			}
			rabbitAddresses = addresses.toArray(new Address[addresses.size()]);
		} else {
			String rabbitHost = XContentMapValues.nodeStringValue(consumerSettings.get("host"),
					"localhost");
			int rabbitPort = XContentMapValues.nodeIntegerValue(consumerSettings.get("port"),
					AMQP.PROTOCOL.PORT);
			rabbitAddresses = new Address[] { new Address(rabbitHost, rabbitPort) };
		}

		rabbitUser = XContentMapValues.nodeStringValue(consumerSettings.get("user"), "guest");
		rabbitPassword = XContentMapValues.nodeStringValue(consumerSettings.get("pass"), "guest");
		rabbitVhost = XContentMapValues.nodeStringValue(consumerSettings.get("vhost"), "/");

		rabbitQueue = XContentMapValues.nodeStringValue(consumerSettings.get("queue"),
				"elasticsearch");
		rabbitExchange = XContentMapValues.nodeStringValue(consumerSettings.get("exchange"),
				"elasticsearch");
		rabbitRoutingKey = XContentMapValues.nodeStringValue(consumerSettings.get("routing_key"),
				"elasticsearch");
		fileDLPath = XContentMapValues.nodeStringValue(
				consumerSettings.get("fileDLPath"), "http://rdfile.gw.com.cn");

		rabbitExchangeDeclare = XContentMapValues.nodeBooleanValue(
				consumerSettings.get("exchange_declare"), true);
		if (rabbitExchangeDeclare) {

			rabbitExchangeType = XContentMapValues.nodeStringValue(
					consumerSettings.get("exchange_type"), "direct");
			rabbitExchangeDurable = XContentMapValues.nodeBooleanValue(
					consumerSettings.get("exchange_durable"), true);
		} else {
			rabbitExchangeType = "direct";
			rabbitExchangeDurable = true;
		}

		rabbitQueueDeclare = XContentMapValues.nodeBooleanValue(
				consumerSettings.get("queue_declare"), true);
		if (rabbitQueueDeclare) {
			rabbitQueueDurable = XContentMapValues.nodeBooleanValue(
					consumerSettings.get("queue_durable"), true);
			rabbitQueueAutoDelete = XContentMapValues.nodeBooleanValue(
					consumerSettings.get("queue_auto_delete"), false);
			if (consumerSettings.containsKey("args")) {
				rabbitQueueArgs = (Map<String, Object>) consumerSettings.get("args");
			}
		} else {
			rabbitQueueDurable = true;
			rabbitQueueAutoDelete = false;
		}
		rabbitQueueBind = XContentMapValues.nodeBooleanValue(consumerSettings.get("queue_bind"),
				true);

		if (consumerSettings.containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) consumerSettings.get("index");
			bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
		} else {
			bulkSize = 100;
		}

		if (consumerSettings.containsKey("bulk_script_filter")) {
			Map<String, Object> scriptSettings = (Map<String, Object>) consumerSettings
					.get("bulk_script_filter");
			if (scriptSettings.containsKey("script")) {
				String scriptLang = "native";
				if (scriptSettings.containsKey("script_lang")) {
					scriptLang = scriptSettings.get("script_lang").toString();
				}
				Map<String, Object> scriptParams = null;
				if (scriptSettings.containsKey("script_params")) {
					scriptParams = (Map<String, Object>) scriptSettings.get("script_params");
				} else {
					scriptParams = Maps.newHashMap();
				}
				bulkScript = scriptService.executable(scriptLang, scriptSettings.get("script")
						.toString(), scriptParams);
			} else {
				bulkScript = null;
			}
		} else {
			bulkScript = null;
		}

		if (consumerSettings.containsKey("script_filter")) {
			Map<String, Object> scriptSettings = (Map<String, Object>) consumerSettings
					.get("script_filter");
			if (scriptSettings.containsKey("script")) {
				String scriptLang = "mvel";
				if (scriptSettings.containsKey("script_lang")) {
					scriptLang = scriptSettings.get("script_lang").toString();
				}
				Map<String, Object> scriptParams = null;
				if (scriptSettings.containsKey("script_params")) {
					scriptParams = (Map<String, Object>) scriptSettings.get("script_params");
				} else {
					scriptParams = Maps.newHashMap();
				}
				script = scriptService.executable(scriptLang, scriptSettings.get("script")
						.toString(), scriptParams);
			} else {
				script = null;
			}
		} else {
			script = null;
		}

		connectionFactory = new ConnectionFactory();
		connectionFactory.setUsername(rabbitUser);
		connectionFactory.setPassword(rabbitPassword);
		connectionFactory.setVirtualHost(rabbitVhost);

		LOG.info("creating datadriver river, addresses [{}], user [{}], vhost [{}]",
				rabbitAddresses, connectionFactory.getUsername(),
				connectionFactory.getVirtualHost());

		bulkProcessor = BulkProcessor.builder(client, listener).setBulkActions(bulkSize)
				.setConcurrentRequests(30).setFlushInterval(TimeValue.timeValueSeconds(1)).build();
	}
	
	private final String CDN_FACK_PATH = "#DZH2DATA#0#";

	public String escapePath(String fpath) {
		if (fpath == null) {
			return null;
		}
		String newPath = fpath.replace(CDN_FACK_PATH, fileDLPath);
		if (!newPath.startsWith("http:")) {
			newPath = null;
		}

		return newPath;
	}
	
	private String getC12Path(JSONObject jo) {
		JSONArray c11 = jo.getJSONArray("C11");
		if (c11 != null) {
			int size = c11.size();
			for (int i = 0; i < size; i++) {
				JSONObject c12 = c11.getJSONObject(i);
				String _c12 = c12.getString("C12");
				if (_c12.endsWith(".pdf") || _c12.endsWith(".docx")
						|| _c12.endsWith(".doc") || _c12.endsWith(".xls")) {
					return _c12;
				}
			}
		}
		return null;
	}

	private void addTask(BulkProcessor bulkRequestBuilder, QueueingConsumer.Delivery task)
			throws Exception {
		String taskData = new String(task.getBody());

		boolean isJSONArray = true;
		JSONArray ja = null;
		try {
			ja = JSONArray.parseArray(taskData);
		} catch (Exception e) {
			isJSONArray = false;
		}

		if (isJSONArray) {
			int len = ja.size();
			for (int i = 0; i < len; i++) {
				JSONObject jo = ja.getJSONObject(i);
				// 仅接收可接收类型
				if (index.equals(jo.get(DataDriverConstants.FIELD_DATASRC))
						&& acceptableTypes.contains(jo.get(DataDriverConstants.FIELD_DATATYPE))) {
					ActionRequest<?> req = parse(jo);
					if (req != null) {
						bulkRequestBuilder.add(req);
					}
				}
			}
		} else {
			JSONObject jo = JSON.parseObject(taskData);
			// 仅接收可接收类型
			if (index.equals(jo.get(DataDriverConstants.FIELD_DATASRC))
					&& acceptableTypes.contains(jo.get(DataDriverConstants.FIELD_DATATYPE))) {
				ActionRequest<?> req = parse(jo);
				if (req != null) {
					bulkRequestBuilder.add(req);
				}
			}
		}
	}

	private ActionRequest<?> parse(JSONObject origDoc) throws Exception {
		String index = origDoc.getString(DataDriverConstants.FIELD_DATASRC);
		String type = origDoc.getString(DataDriverConstants.FIELD_DATATYPE);
		String cmd = origDoc.getString(DataDriverConstants.FIELD_OP_TYPE).toLowerCase();

		String id = origDoc.getString(DataDriverConstants.FIELD_ID);
		if (id == null) {
			LOG.info("this doc don't have id.");
			return null;
		}

		// 创建或更新都是索引
		if (cmd.equalsIgnoreCase(DataDriverConstants.INDEX_CMD_UPDATE)) {
			cmd = "index";
		} else if (cmd.equalsIgnoreCase(DataDriverConstants.INDEX_CMD_DELETE)) {
			cmd = "delete";
		} else {
			cmd = "index";
		}

		IDocumentParser parser = parserFactory.getParser(type);
		// ******************************//
		// 替换URL，使其能够正常访问
		origDoc.put("C4", escapePath(origDoc.getString("C4")));
		origDoc.put("C15", escapePath(origDoc.getString("C15")));
		origDoc.put("C12Path", escapePath(getC12Path(origDoc)));
		// ******************************//
		// 文档转换
		JSONObject dataDoc = parser.parseDocument(origDoc);
		if (dataDoc != null) {
			return IndexUtil.genBulkCmd(cmd, index, type, id, dataDoc);
		}
		return null;
	}

	private void cleanup(int code, String message) {
		try {
			channel.close(code, message);
		} catch (Exception e) {
			LOG.debug("failed to close channel on [{}]", e, message);
		}
		try {
			connection.close(code, message);
		} catch (Exception e) {
			LOG.debug("failed to close connection on [{}]", e, message);
		}
	}

	private void processBody(QueueingConsumer.Delivery task, BulkProcessor bulk) throws Exception {
		byte[] body = task.getBody();
		if (body == null)
			return;

		if (bulkScript == null) {
			if (script == null) {
				addTask(bulk, task);
				// bulkRequestBuilder.add(body, 0, body.length, false);
			} else {
				// #26: add support for doc by doc scripting
				processBodyPerLine(body, bulk);
			}
		} else {
			String bodyStr = new String(body);
			bulkScript.setNextVar("body", bodyStr);
			String newBodyStr = (String) bulkScript.run();
			if (newBodyStr != null) {
				byte[] newBody = newBodyStr.getBytes();
				bulk.add(new BytesArray(newBody), false, null, null);
			}
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	private void processBodyPerLine(byte[] body, BulkProcessor bulkRequestBuilder) throws Exception {
		BufferedReader reader = new BufferedReader(new StringReader(new String(body)));

		JsonFactory factory = new JsonFactory();
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			JsonXContentParser parser = null;
			try {
				parser = new JsonXContentParser(factory.createJsonParser(line));
				Map<String, Object> asMap = parser.map();

				if (asMap.get("delete") != null) {
					// We don't touch deleteRequests
					String newContent = line + "\n";
					bulkRequestBuilder
							.add(new BytesArray(newContent.getBytes()), false, null, null);
				} else {
					// But we send other requests to the script Engine in
					// ctx
					// field
					Map<String, Object> ctx = null;
					String payload = null;
					try {
						payload = reader.readLine();
						ctx = XContentFactory.xContent(XContentType.JSON).createParser(payload)
								.mapAndClose();
					} catch (IOException e) {
						LOG.warn("failed to parse {}", e, payload);
						continue;
					}
					script.setNextVar("ctx", ctx);
					script.run();
					ctx = (Map<String, Object>) script.unwrap(ctx);
					if (ctx != null) {
						// Adding header
						StringBuffer request = new StringBuffer(line);
						request.append("\n");
						// Adding new payload
						request.append(XContentFactory.jsonBuilder().map(ctx).string());
						request.append("\n");

						if (LOG.isTraceEnabled()) {
							LOG.trace("new bulk request is now: {}", request.toString());
						}
						byte[] binRequest = request.toString().getBytes();
						bulkRequestBuilder.add(new BytesArray(binRequest), false, null, null);
					}
				}
			} catch (Exception e) {
				LOG.warn("failed to process line", e);
			} finally {
				if (parser != null)
					parser.close();
			}
		}
	}

	private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
		@Override
		public void beforeBulk(long executionId, BulkRequest request) {
			//
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			//
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			//
		}
	};

	@Override
	public void doStart() {
		// 先解析设置信息
		try{
			resolveSettings();
		}catch(Exception e){
			LOG.warn("resolveSettings exception.", e);
		}

		while (true) {
			if (closed) {
				break;
			}
			try {
				connection = connectionFactory.newConnection(rabbitAddresses);
				channel = connection.createChannel();
			} catch (Exception e) {
				if (!closed) {
					LOG.warn("failed to created a connection / channel", e);
				} else {
					continue;
				}
				cleanup(0, "failed to connect");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// ignore, if we are closing, we will exit later
				}
			}

			QueueingConsumer consumer = new QueueingConsumer(channel);
			// define the queue
			try {
				if (rabbitQueueDeclare) {
					// only declare the queue if we should
					channel.queueDeclare(rabbitQueue/* queue */, rabbitQueueDurable/* durable */,
							false/* exclusive */, rabbitQueueAutoDelete/* autoDelete */,
							rabbitQueueArgs/*
											 * extra args
											 */);
				}
				if (rabbitExchangeDeclare) {
					// only declare the exchange if we should
					channel.exchangeDeclare(rabbitExchange/* exchange */, rabbitExchangeType/* type */,
							rabbitExchangeDurable);
				}
				if (rabbitQueueBind) {
					// only bind queue if we should
					channel.queueBind(rabbitQueue/* queue */, rabbitExchange/* exchange */,
							rabbitRoutingKey/* routingKey */);
				}
				channel.basicConsume(rabbitQueue/* queue */, false/* noAck */, consumer);
			} catch (Exception e) {
				if (!closed) {
					LOG.warn("failed to create queue [{}]", e, rabbitQueue);
				}
				cleanup(0, "failed to create queue");
				continue;
			}

			// now use the queue to listen for messages
			while (true) {
				if (closed) {
					break;
				}
				QueueingConsumer.Delivery task;
				try {
					// 读一条
					task = consumer.nextDelivery();
				} catch (Exception e) {
					if (!closed) {
						LOG.error("failed to get next message, reconnecting...", e);
					}
					cleanup(0, "failed to get message");
					break;
				}

				if (task != null && task.getBody() != null) {
					try {
						processBody(task, bulkProcessor);
					} catch (Exception e) {
						LOG.warn("failed to parse request for delivery tag [{}], ack'ing...", e,
								task.getEnvelope().getDeliveryTag());
					} finally {
						try {
							channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
						} catch (IOException e1) {
							LOG.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
						}
					}
				}
			}
		}
		cleanup(0, "closing river");
	}

	@Override
	public String name() {
		return "rabbitmq";
	}

	@Override
	public void doStop() {
		try {
			closed = true;
			channel.close();
			connection.close();
		} catch (IOException e) {
			LOG.warn("error when close rabbitmq consumer", e);
		}
	}
}

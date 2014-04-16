/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package me.boyce.elasticsearch.dd.river;

import me.boyce.elasticsearch.dd.river.pull.IRiverPuller;
import me.boyce.elasticsearch.dd.river.push.parser.DefaultDocumentParserFactory;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParser;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParserFactory;
import me.boyce.elasticsearch.dd.river.push.parser.IRiverConsumer;
import me.boyce.elasticsearch.dd.util.RiverServiceLoader;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverException;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

/**
 * 数据驱动River组件，包括数据的拉取与推送两种获取方式。推送部分由RabbitMQ组件来完成，拉取部分则依据具体应用实现。
 * <p>
 * 注意：一般业务数据由两部分组成：历史数据+最新数据。对于搜索引擎而言，需要保证数据的完整性和及时性。因此，
 * 这里专门设计实现了一个数据驱动引擎。该引擎包括两个组件，一是拉取组件，用户对历史数据进行批量拉取，这个过程通常在
 * River组件被第一次创建时进行，从而保证历史数据的完整性，增量产生的新数据则通过推送方式接收，常用的做法是通过RabbitMQ
 * 来完成。通过推拉结合的方式，可以有效保证数据的完整性和实时性。（当然，数据源需要保证数据ID号的唯一性和持久性，从而
 * 避免拉取和推送时重复数据的产生。）</br>
 * 当然，拉取方面的实现，可以比较灵活，只需实现IRiverPuller接口或继承AbstractRiverPuller类
 * ，在run方法中自行实现具体操作。可以 实现为单次拉取（配合推送保持数据完整性）或增量拉取（可以拿掉推送实现），具体根据业务特点进行即可。
 *
 * @author boyce
 * @created: 2013-6-21 下午3:30:52
 * @version 0.1
 *
 */
public class DataDriverRiver extends AbstractRiverComponent implements River {

	private IRiverPuller riverPuller;
	private IRiverConsumer riverConsumer;

	// =============================================
	// 全局解析模块
	// =============================================
	private Set<String> acceptableTypes = new HashSet<>();
	private String index = null;

	// =============================================
	// 数据解析模块配置
	// =============================================
	private volatile boolean closed = false;

	private volatile Thread consumerThread;
	private volatile Thread pullerThread;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public DataDriverRiver(RiverName riverName, RiverSettings settings, Client client,
			ScriptService scriptService) {
		super(riverName, settings);
		IDocumentParserFactory parserFactory = null;

		// ===== 全局设置信息
		Map<String, Object> globalSettings = (Map<String, Object>) settings.settings().get(
				"settings");
		Map<String, Object> mappings = null;
		Map<String, Object> index_setting = null;
		if (globalSettings != null) {
			// 索引名
			index = (String) globalSettings.get("index");
			// 可接受类型
			List<String> lst = (List<String>) globalSettings.get("types");
			for (String type : lst) {
				acceptableTypes.add(type);
			}
			// mapping信息
			mappings = (Map<String, Object>) globalSettings.get("mappings");
			//索引设置
			index_setting = (Map<String, Object>) globalSettings.get("index_setting");			
			Map<String, Object> parserFactorySettings = (Map<String, Object>) globalSettings
					.get("parserFactory");
			if (parserFactorySettings != null) {
				String factory = (String) parserFactorySettings.get("name");
				if (factory != null) {
					// 默认解析工厂
					if (factory.equals("default")) {
						DefaultDocumentParserFactory f = new DefaultDocumentParserFactory();
						parserFactory = f;
						Map<String, String> map = (Map<String, String>) parserFactorySettings
								.get("parsers");
						if (map != null) {
							Map<String, IDocumentParser> parsers = new HashMap<String, IDocumentParser>();
							for (String key : map.keySet()) {
								String className = map.get(key);
								try {
									parsers.put(key, (IDocumentParser) Class.forName(className)
											.newInstance());
								} catch (Exception e) {
									logger.error("error when create new instance for parser: {}",
											e, className);
								}
							}
							f.setParsers(parsers);
						}
					} else {
						// 用户自定义解析工厂
						try {
							parserFactory = (IDocumentParserFactory) Class.forName(factory)
									.newInstance();
						} catch (Exception e) {
							logger.warn("failed to create parser factory: {}", factory);
						}
					}
				}
			}
		}

		// ===== 设置拉取参数
		Map<String, Object> pullerSettings = (Map<String, Object>) settings.settings()
				.get("puller");
		if (pullerSettings != null) {
			String pullerName = (String) pullerSettings.get("name");
			riverPuller = RiverServiceLoader.getNameableInstance(pullerName, IRiverPuller.class);
			if (riverPuller == null) {
				logger.error("error when lookup IRiverPuller with name: {}", pullerName);
			} else {
				try {
					// 添加设置
					riverPuller.client(client);
					riverPuller.riverName(riverName.getName());
					riverPuller.index(index);
					riverPuller.parserFactory(parserFactory);
					riverPuller.acceptableTypes(acceptableTypes);
					riverPuller.scriptService(scriptService);
					riverPuller.mappings(mappings);
					riverPuller.indexSetting(index_setting);
					riverPuller.settings((Map<String, Object>) pullerSettings.get("settings"));
				} catch (Exception e) {
					throw new RiverException(riverName,
							"error when parser settings for riverPuller: " + pullerName
									+ ", settings: " + settings, e);
				}
			}
		}

		// ===== 设置rabbitMQ推送接口
		Map<String, Object> consumberSettings = (Map<String, Object>) settings.settings().get(
				"consumer");
		if (consumberSettings != null) {
			String consumerName = consumberSettings.get("name").toString();
			Map<String, Object> riverConsumerSettings = (Map<String, Object>) consumberSettings
					.get("settings");
			riverConsumer = RiverServiceLoader.getNameableInstance(consumerName,
					IRiverConsumer.class);
			if (riverConsumer == null) {
				logger.error("error when lookup IRiverConsumer with name: {}", consumerName);
			} else {
				try {
					riverConsumer.client(client);
					riverConsumer.riverName(riverName.getName());
					riverConsumer.index(index);
					riverConsumer.parserFactory(parserFactory);
					riverConsumer.scriptService(scriptService);
					riverConsumer.acceptableTypes(acceptableTypes);
					riverConsumer.mappings(mappings);
					riverConsumer.indexSetting(index_setting);
					riverConsumer.settings(riverConsumerSettings);
				} catch (Exception e) {
					throw new RiverException(riverName,
							"error when parser settings for riverConsumer: " + consumerName
									+ ", settings: " + settings, e);
				}
			}
		}

		if (index == null && index.length() == 0) {
			throw new RiverException(riverName, "missing index parameter");
		}
		if (acceptableTypes.size() == 0) {
			throw new RiverException(riverName, "missing types parameter");
		}

		if (parserFactory == null)
			parserFactory = new DefaultDocumentParserFactory();
	}

	@Override
	public void start() {

		boolean init = false;

		ThreadFactory tf = EsExecutors.daemonThreadFactory(settings.globalSettings(),
				"datadriver_river");

		if (riverConsumer != null) {

			riverConsumer.checkAndCreateIndex(index);
			riverConsumer.setMappings();
			init = true;

			consumerThread = tf.newThread(riverConsumer);
			consumerThread.start();
		}

		if (riverPuller != null) {

			if(!init){
				riverPuller.checkAndCreateIndex(index);
				riverPuller.setMappings();
			}

			pullerThread = tf.newThread(riverPuller);
			pullerThread.start();
		}
	}

	@Override
	public void close() {
		if (closed) {
			return;
		}
		logger.info("closing datadriver river");
		closed = true;
		if (riverConsumer != null) {
			riverConsumer.stop();
			consumerThread.interrupt();
		}
		if (riverPuller != null) {
			riverPuller.stop();
			pullerThread.interrupt();
		}
		if (pullerThread != null) {
			pullerThread.interrupt();
		}
		if (consumerThread != null) {
			consumerThread.interrupt();
		}
	}
}

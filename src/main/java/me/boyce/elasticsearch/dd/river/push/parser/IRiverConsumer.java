package me.boyce.elasticsearch.dd.river.push.parser;

import java.util.Map;
import java.util.Set;

import me.boyce.elasticsearch.dd.common.Nameable;

import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;

/**
 * river推送的消费者接口
 *
 * @author boyce
 * @created: 2013-6-22 下午3:53:40
 * @version 0.1
 *
 */
public interface IRiverConsumer extends Runnable, Nameable {
	/**
	 *
	 * @param riverName
	 * @return
	 */
	IRiverConsumer riverName(String riverName);

	/**
	 * 设置ES客户端
	 *
	 * @param client
	 * @return
	 */
	IRiverConsumer client(Client client);

	/**
	 * 指定索引名
	 *
	 * @param index
	 * @return
	 */
	IRiverConsumer index(String index);

	/**
	 * 用户自定义设置信息
	 *
	 * @param settings
	 */
	IRiverConsumer settings(Map<String, Object> settings);

	/**
	 * mapping设置信息
	 *
	 * @param mappings
	 * @return
	 * @throws Exception
	 */
	IRiverConsumer mappings(Map<String, Object> mappings) throws Exception;
	
	/**
	 * index设置
	 *
	 * @param indexSetting
	 * @return
	 * @throws Exception
	 */
	IRiverConsumer indexSetting(Map<String, Object> indexSetting)
			throws Exception;

	/**
	 * 设置文档解析工厂
	 *
	 * @param factory
	 * @return
	 */
	IRiverConsumer parserFactory(IDocumentParserFactory factory);

	/**
	 * ES脚本服务功能
	 *
	 * @param scriptService
	 * @return
	 */
	IRiverConsumer scriptService(ScriptService scriptService);

	/**
	 * 设置可接受的数据类型
	 *
	 * @param types
	 * @return
	 */
	IRiverConsumer acceptableTypes(Set<String> types);

	/**
	 * 停止接收数据
	 */
	void stop();

	void checkAndCreateIndex(String index);
	void setMappings();
}

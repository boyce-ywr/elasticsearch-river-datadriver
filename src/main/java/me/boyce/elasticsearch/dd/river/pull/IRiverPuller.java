package me.boyce.elasticsearch.dd.river.pull;

import java.util.Map;
import java.util.Set;

import me.boyce.elasticsearch.dd.common.Nameable;
import me.boyce.elasticsearch.dd.river.push.parser.IDocumentParserFactory;

import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;

/**
 * 数据拉取接口。注意，实现中，实现类的发现通过ServiceLoader来完成，ServiceLoader要求在META-
 * INF目录下的services目录提供继承关系的映射文件
 * 。具体实现人员在提供自定义扩展包时，请在按照当前jar中的META-INF目录下services目录来打包
 * 
 * @author boyce
 * @created: 2013-6-22 下午3:52:04
 * @version 0.1
 * 
 */
public interface IRiverPuller extends Runnable, Nameable {
	/**
	 * 
	 * @param riverName
	 * @return
	 */
	IRiverPuller riverName(String riverName);

	/**
	 * 设置ES客户端
	 * 
	 * @param client
	 * @return
	 */
	IRiverPuller client(Client client);

	/**
	 * 索引名
	 * 
	 * @param idx
	 * @return
	 */
	IRiverPuller index(String idx);

	/**
	 * 用户自定义设置信息
	 * 
	 * @param settings
	 */
	IRiverPuller settings(Map<String, Object> settings) throws Exception;

	/**
	 * mapping设置信息
	 * 
	 * @param mappings
	 * @return
	 * @throws Exception
	 */
	IRiverPuller mappings(Map<String, Object> mappings) throws Exception;
	
	/**
	 * index设置
	 *
	 * @param indexSetting
	 * @return
	 * @throws Exception
	 */
	IRiverPuller indexSetting(Map<String, Object> indexSetting)
			throws Exception;

	/**
	 * 设置文档解析工厂
	 * 
	 * @param factory
	 * @return
	 */
	IRiverPuller parserFactory(IDocumentParserFactory factory);

	/**
	 * ES脚本服务功能
	 * 
	 * @param scriptService
	 * @return
	 */
	IRiverPuller scriptService(ScriptService scriptService);

	/**
	 * 设置可接受的数据类型
	 * 
	 * @param types
	 * @return
	 */
	IRiverPuller acceptableTypes(Set<String> types);

	/**
	 * 停止工作
	 */
	void stop();
	
	void checkAndCreateIndex(String index);
	void setMappings();
}

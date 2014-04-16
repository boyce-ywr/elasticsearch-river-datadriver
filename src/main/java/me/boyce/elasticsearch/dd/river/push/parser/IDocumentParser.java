package me.boyce.elasticsearch.dd.river.push.parser;

import me.boyce.elasticsearch.dd.common.Nameable;

import com.alibaba.fastjson.JSONObject;

/**
 * 文档解析器
 * 
 * @author boyce
 * @created: 2013-6-21 下午4:00:36
 * @version 0.1
 * 
 */
public interface IDocumentParser extends Nameable {

	/**
	 * 将单个文档的数据取出来转换为正确的格式
	 * 
	 * @param doc
	 * @return
	 * @throws Exception
	 */
	JSONObject parseDocument(String doc) throws Exception;

	/**
	 * 
	 * @param jo
	 * @return
	 * @throws Exception
	 */
	JSONObject parseDocument(JSONObject jo) throws Exception;
}

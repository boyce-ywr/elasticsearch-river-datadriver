/**
 *
 */
package me.boyce.elasticsearch.dd.river.push.parser;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 基本数据解析器。可以继承当前对象对更加详细的业务进行额外处理。
 * 
 * @author KunBetter
 * 
 */
public class DefaultDocumentParser implements IDocumentParser {

	@Override
	public String name() {
		return "default";
	}

	@Override
	public JSONObject parseDocument(String doc) throws Exception {
		return parseDocument(JSON.parseObject(doc));
	}

	/**
	 * 默认实现很简单，就是将非指令字段全部取出来，不进行任何处理
	 */
	@Override
	public JSONObject parseDocument(JSONObject doc) throws Exception {
		JSONObject body = new JSONObject();
		for (String key : doc.keySet()) {
			if (key.equals(DataDriverConstants.FIELD_DATASRC)
					|| key.equals(DataDriverConstants.FIELD_DATATYPE)
					|| key.equals(DataDriverConstants.FIELD_OP_TYPE)) {
				continue;
			}
			body.put(key, doc.get(key));
		}
		return body;
	}
}

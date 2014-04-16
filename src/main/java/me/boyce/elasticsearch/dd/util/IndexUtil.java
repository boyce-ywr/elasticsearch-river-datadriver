package me.boyce.elasticsearch.dd.util;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;
import me.boyce.elasticsearch.dd.hash.Hashing;
import me.boyce.elasticsearch.dd.hash.MurmurHash;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;

import com.alibaba.fastjson.JSONObject;

public class IndexUtil {

	private final static Hashing GEN64UID = new MurmurHash();

	private static String genId(String str2Hash) {
		return String.valueOf(GEN64UID.hash(str2Hash) & 0x7FFFFFFFFFFFFFFFL);
	}

	/**
	 * 根据来源的index，type和id生成es的索引ID
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public static String genId(String index, String type, String id) {
		return genId(index + type + id);
	}

	private static boolean isBlank(String str) {
		return str == null || str.length() == 0 || str.trim().length() == 0;
	}

	/**
	 * 生成索引命令
	 * 
	 * @param cmd
	 * @param index
	 * @param type
	 * @param id
	 * @param doc
	 *            解析完成的文档
	 * @return
	 * @throws Exception
	 */
	public static JSONObject genIndexCmd(String cmd, String index, String type,
			String id, JSONObject doc) throws Exception {
		JSONObject meta = new JSONObject();
		if (isBlank(cmd))
			index = (String) doc.get(DataDriverConstants.FIELD_DATASRC);
		if (isBlank(type))
			type = (String) doc.get(DataDriverConstants.FIELD_DATATYPE);
		if (isBlank(id))
			id = (String) doc.get(DataDriverConstants.FIELD_ID);

		meta.put("_index", index);
		meta.put("_type", type);
		meta.put("_id", genId(index, type, id));
		JSONObject root = new JSONObject();
		root.put(cmd, meta);
		root.put(type, doc);
		return root;
	}

	/**
	 * 生成bulk命令
	 * 
	 * @param cmd
	 * @param index
	 * @param type
	 * @param id
	 * @param doc
	 * @return
	 * @throws Exception
	 */
	public static ActionRequest<?> genBulkCmd(String cmd, String index,
			String type, String id, JSONObject doc) throws Exception {

		if (cmd.equals("delete")) {
			DeleteRequest req = new DeleteRequest(index, type, id);
			return req;
		} else {
			IndexRequest req = new IndexRequest(index, type, id);
			req.source(doc.toJSONString());
			return req;
		}
	}
}

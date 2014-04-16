package me.boyce.elasticsearch.dd.river.push.parser;

/**
 * 文档解析器工厂接口
 * 
 * @author boyce
 * @created: 2013-6-22 下午2:49:35
 * @version 0.1
 * 
 */
public interface IDocumentParserFactory {

	/**
	 * 根据文档类型获取对应的解析器
	 * 
	 * @param type
	 * @return
	 */
	IDocumentParser getParser(String type);
}

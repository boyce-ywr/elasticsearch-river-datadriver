package me.boyce.elasticsearch.dd.util;

import java.util.Iterator;
import java.util.ServiceLoader;

import me.boyce.elasticsearch.dd.common.Nameable;

import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.ESLogger;

/**
 * 使用ServiceLoader进行服务组件的动态发现
 * 
 * @author boyce
 * @created: 2013-6-22 下午3:56:03
 * @version 0.1
 * 
 */
public class RiverServiceLoader {

	public static final ESLogger LOG = ESLoggerFactory
			.getLogger(RiverServiceLoader.class.getName());

	public static <T extends Nameable> T getNameableInstance(String name,
			Class<T> cls) {
		ServiceLoader<T> loader = ServiceLoader.load(cls);
		Iterator<T> it = loader.iterator();
		int cnt = 0;
		while (it.hasNext()) {
			cnt++;
			T t = it.next();
			if (t.name().equals(name)) {
				return t;
			}
		}
		LOG.info("RiverServiceLoader, count: {}", cnt);
		return null;
	}

}

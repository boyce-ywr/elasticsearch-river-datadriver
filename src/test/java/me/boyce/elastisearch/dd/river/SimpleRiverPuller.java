package me.boyce.elastisearch.dd.river;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import me.boyce.elasticsearch.dd.river.pull.AbstractRiverPuller;
import me.boyce.elasticsearch.dd.river.pull.IRiverPuller;

import org.elasticsearch.common.Classes;

import com.alibaba.fastjson.JSONObject;

public class SimpleRiverPuller extends AbstractRiverPuller {

	private BufferedReader reader = null;

	@Override
	public String name() {
		return "simple";
	}

	@Override
	public IRiverPuller settings(Map<String, Object> settings) throws Exception {
		super.settings(settings);
		reader = new BufferedReader(new InputStreamReader(Classes.getDefaultClassLoader().getResourceAsStream((String) settings.get("file"))));
		return this;
	}

	@Override
	protected void doStart() {
		try {
			String line = reader.readLine();
			long id = 1L;
			while (line != null) {
				line = reader.readLine();
				JSONObject jo = new JSONObject();
				jo.put("__id", String.valueOf(id++));
				jo.put("content", line);
				addToIndex(jo, name());
			}
			flush();
			LOG.info("finished to read all data from file");
		} catch (IOException e) {
			LOG.error("error when read file", e);
		}
	}

	@Override
	protected void doStop() {
		try {
			reader.close();
		} catch (IOException e) {
			// ignore
		}
	}

}

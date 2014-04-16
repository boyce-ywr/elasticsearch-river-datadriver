package me.boyce.elasticsearch.dd.plugin.river;

import me.boyce.elasticsearch.dd.constant.DataDriverConstants;
import me.boyce.elasticsearch.dd.river.DataDriverRiverModule;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

/**
 * 数据驱动River组件，包括数据的拉取与推送两种获取方式
 * 
 * @author boyce
 * @created: 2013-6-21 下午3:30:05
 * @version 0.1
 * 
 */
public class DataDriverRiverPlugin extends AbstractPlugin {

	@Inject
	public DataDriverRiverPlugin() {
	}

	@Override
	public String name() {
		return "river-datadriver";
	}

	@Override
	public String description() {
		return "River DataDriver Plugin";
	}

	public void onModule(RiversModule module) {
		module.registerRiver(DataDriverConstants.RIVER_NAME, DataDriverRiverModule.class);
	}
}

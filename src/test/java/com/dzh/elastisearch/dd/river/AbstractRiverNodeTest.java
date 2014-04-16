package com.dzh.elastisearch.dd.river;

import static org.elasticsearch.common.collect.Maps.newHashMap;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class AbstractRiverNodeTest {

	private static final ESLogger LOG = Loggers
			.getLogger(AbstractRiverNodeTest.class);

	public final String INDEX = "my_dd_river";
	public final String TYPE = "my_dd_river";
	private Map<String, Node> nodes = newHashMap();
	private Map<String, Client> clients = newHashMap();

	private Settings defaultSettings = ImmutableSettings
			.settingsBuilder()
            .put("path.data", "target/data")
			.put("cluster.name",
					"test-cluster-"
							+ NetworkUtils.getLocalAddress().getHostName())
			.build();

	@BeforeMethod
	public void createIndices() throws Exception {
		startNode("1").client();
		client("1").admin().indices().create(new CreateIndexRequest(INDEX))
				.actionGet();
	}

	@AfterMethod
	public void deleteIndices() {
		try{
		client("1").admin().indices()
				.delete(new DeleteIndexRequest().indices(INDEX)).actionGet();
		}catch(IndexMissingException e){
			LOG.error(e.getMessage());
		}
		try {
            // clear rivers
            client("1").admin().indices()
                    .delete(new DeleteIndexRequest().indices("_river"))
                    .actionGet();
        } catch (IndexMissingException e) {
            LOG.error(e.getMessage());
        }
		closeNode("1");
		closeAllNodes();
	}

	public void putDefaultSettings(Settings.Builder settings) {
		putDefaultSettings(settings.build());
	}

	public void putDefaultSettings(Settings settings) {
		defaultSettings = ImmutableSettings.settingsBuilder()
				.put(defaultSettings).put(settings).build();
	}

	public Node startNode(String id) {
		return buildNode(id).start();
	}

	public Node startNode(String id, Settings.Builder settings) {
		return startNode(id, settings.build());
	}

	public Node startNode(String id, Settings settings) {
		return buildNode(id, settings).start();
	}

	public Node buildNode(String id) {
		return buildNode(id, EMPTY_SETTINGS);
	}

	public Node buildNode(String id, Settings.Builder settings) {
		return buildNode(id, settings.build());
	}

	public Node buildNode(String id, Settings settings) {
		Settings finalSettings = settingsBuilder().put(defaultSettings)
				.put(settings).put("name", id).build();
		if (finalSettings.get("gateway.type") == null) {
			// default to non gateway
			finalSettings = settingsBuilder().put(finalSettings)
					.put("gateway.type", "none").build();
		}
		if (finalSettings.get("cluster.routing.schedule") != null) {
			// decrease the routing schedule so new nodes will be added quickly
			finalSettings = settingsBuilder().put(finalSettings)
					.put("cluster.routing.schedule", "50ms").build();
		}
		Node node = nodeBuilder().settings(finalSettings).build();
		nodes.put(id, node);
		clients.put(id, node.client());
		return node;
	}

	public void closeNode(String id) {
		Client clt = clients.remove(id);
		if (clt != null) {
			clt.close();
		}
		Node node = nodes.remove(id);
		if (node != null) {
			node.close();
		}
	}

	public Node node(String id) {
		return nodes.get(id);
	}

	public Client client(String id) {
		return clients.get(id);
	}

	public void closeAllNodes() {
		for (Client client : clients.values()) {
			client.close();
		}
		clients.clear();
		for (Node node : nodes.values()) {
			node.close();
		}
		nodes.clear();
	}
}

# ElasticSearch 数据驱动插件
@Author 杨伟荣

@Contact boyce.ywr@gmail.com

数据驱动River组件，包括数据的拉取与推送两种获取方式。

一般业务数据由两部分组成：历史数据+最新数据。对于搜索引擎而言，需要保证数据的完整性和及时性。因此，
这里专门设计实现了一个数据驱动引擎。

该引擎包括两个组件：
	一是拉取组件，用户对历史数据进行批量拉取，这个过程通常在River组件被第一次创建时进行，从而保证历史数据的完整性；
	二是推送接收组件，增量产生的新数据则通过推送方式接收，常用的做法是通过RabbitMQ来完成。通过推拉结合的方式，可以有效保证数据的完整性和实时性。（当然，数据源需要保证数据ID号的唯一性和持久性，从而
避免拉取和推送时重复数据的产生。）

数据拉取接口被抽象为IRiverPuller接口，并提供了一个抽象实现类AbstractRiverPuller。因为拉取涉及到具体的业务，这里并没有提供出默认实现，相关实现可以参考dd-shequ项目。
数据推送接口被抽象为IRiverConsumer接口，并提供了一个抽象实现类AbstractRiverConsumer，同时提供了一个默认的rabbitMQ的实现方式RabbitMQRiverConsumer。推送方面，默认使用RabbitMQ的实现即可。

数据拉取和推送都是在独立线程中执行的。具体实现为单次拉取还是增量拉取，自行依据业务特点在IRiverPuller的run方法或AbstractRiverPuller的doStart方法中执行即可。

关于数据的解析。一般而言使用默认的parserFactory即可，不用为它设置parsers。但是涉及到对具体数据内容的转换时，则需要自行实现parser，并设置到parserFactory中。譬如，如果推送的字段是关于某个文件的url，如果需要对该文件进行搜索，则需要将该字段的url转换为文件内容，操作应当是通过该url获取到文件内容，然后新增一个文件内容的字段，值设置为该文件内容的Base64编码。

对于绝大部分情况而言，parserFactory设置为default即可，如果需要对特定type类型的数据进行处理，则为该type类型数据实现IDocumentParser接口，实现parseDocument方法，或继承DefaultDocumentParser，重新实现mergeIndexBody方法。

下面是关于river配置的说明：

```
curl -XPUT 'localhost:9200/_river/river_dd_shequ/_meta' -d '{
    "type": "datadriver",	// 指定river类型，这里必须是datadriver
	"consumer": {	// 推送接收实现，一般而言，参照下面即可
		"name": "rabbitmq",
		"settings": {
			"servers" : "10.15.144.31,10.15.144.35,10.15.144.39",
			"port": 5672,
			"user": "guest",
			"pass": "guest",
			"vhost": "/",
			"queue": "elasticsearch",
			"exchange": "dzhseTESTshequ",
			"routing_key": "elasticsearch",
			"exchange_declare": true,
			"exchange_type": "fanout",
			"exchange_durable": true,
			"queue_declare": true,
			"queue_bind": true,
			"queue_durable": true,
			"queue_auto_delete": false
		}
    },
	"puller": {	// 拉取相关设置
		"name": "shequ",	// 对应到你实现的IRiverPuller的name属性
		"settings": {	// 拉取接口的设置信息，作为Map的形式传递给IRiverPuller实现，具体内容根据实际业务使用填写
			"srcHost": "10.15.108.154",
			"srcPort": 1234,
			"typeSettings": {
				"user": {
					"init": "2013-01-01 00:00:00",
					"singleLimit": 10000
				}
			}
		}
	},
	"settings": {	// 全局设置信息
		"index": "shequ",	// 存储数据的索引名，必填
		"types": [	// 可接受的数据类型，必填，如果接收到不是这里填充的type数据，会被忽略，不会送入索引！
			"user",
			"stock",
			"weibo"
		],
		"parserFactory": {	// type数据解析工厂，默认即可
			"name": "default"	// 设置为default
			"parsers":{	// 自定义type类型解析器。一般不用填写，涉及到需要对特殊字段进行处理时实现即可
				"user": "com.dzh.shequ.es.UserDocumentParser"
			}
		},
		"mappings":{	// 各type类型数据的mapping设置，强烈建议为每一个type都指定mapping！
			"user": {
				"_meta": {
					"author": "KunBetter"
				},
				"_all": {
					"enabled": true,
					"index_analyzer": "ik_max_word",
					"search_analyzer": "ik_smart"
				},
				"_source": {
					"enabled": true,
					"compress": true
				},
				"properties": {
					"__id": {
						"type": "string",
						"index": "not_analyzed",
						"store": "yes"
					},
					"r": {
						"type": "integer",
						"index": "not_analyzed",
						"store": "yes"
					},
					"n": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 20
					},
					"gd": {
						"type": "integer",
						"index": "not_analyzed",
						"store": "yes"
					},
					"rn": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 2
					},
					"py": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 20
					},
					"sg": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 18
					},
					"l": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 10
					},
					"pt": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"boost": 15
					},
					"fz": {
						"type": "integer",
						"index": "not_analyzed",
						"store": "yes"
					},
					"ct": {
						"type": "date",
						"format": "YYYY-MM-dd HH:mm:ss",
						"store": "yes"
					},
					"ut": {
						"type": "date",
						"format": "YYYY-MM-dd HH:mm:ss",
						"store": "yes"
					}
				}
			},
			"stock": {
				"_meta": {
					"author": "KunBetter"
				},
				"_all": {
					"enabled": true,
					"index_analyzer": "ik_max_word",
					"search_analyzer": "ik_smart"
				},
				"_source": {
					"enabled": true,
					"compress": true
				},
				"properties": {
					"__id": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart"
					},
					"name": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart"
					}
				}
			},
			"weibo": {
				"_meta": {
					"author": "KunBetter"
				},
				"_all": {
					"enabled": true,
					"index_analyzer": "ik_max_word",
					"search_analyzer": "ik_smart"
				},
				"_source": {
					"enabled": true,
					"compress": true
				},
				"_ttl": {
					"enable": true,
					"default": "52w"
				},
				"properties": {
					"__id": {
						"type": "string",
						"index": "not_analyzed",
						"store": "yes"
					},
					"uid": {
						"type": "string",
						"index": "not_analyzed",
						"store": "yes"
					},
					"t": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 15
					},
					"title": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 20
					},
					"content": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"term_vector": "with_positions_offsets",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 10
					},
					"stock": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 2
					},
					"tag": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 2
					},
					"topic": {
						"type": "string",
						"index": "analyzed",
						"store": "yes",
						"index_analyzer": "ik_max_word",
						"search_analyzer": "ik_smart",
						"boost": 2
					},
					"d": {
						"type": "date",
						"format": "YYYY-MM-dd HH:mm:ss",
						"store": "yes"
					},
					"location": {
						"type": "geo_point"
					},
					"client": {
						"type": "integer",
						"index": "not_analyzed",
						"store": "yes"
					},
					"app": {
						"type": "string",
						"index": "not_analyzed",
						"store": "yes"
					}
				}
			}
		}
	}	
}'
```
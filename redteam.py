
# coding=utf-8
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
import argparse
from util import timetransfer,analyze;

# ES索引和Type名称
INDEX_NAME = "index-redteam"
TYPE_NAME = "log"

# ES操作工具类
class es_tool():
    # 类初始化函数
    def __init__(self, hosts, timeout):
        self.es = Elasticsearch(hosts, timeout=5000)
        pass

    # 将数据存储到es中
    def set_data(self, fields_data=[], index_name=INDEX_NAME, doc_type_name=TYPE_NAME):
        # 创建ACTIONS
        ACTIONS = []
        # print "es set_data length",len(fields_data)
        for fields in fields_data:
            # print "fields", fields
            # print fields[1]
            action = {
                "_index": index_name,
                "_type": doc_type_name,
                "_source": {
                    "@timestamp":timetransfer(fields[0]),
                    "time": fields[0],
                    "user&domain": fields[1],
                    "source_computer": fields[2],
                    "destination_computer": fields[3],
                }
            }
            ACTIONS.append(action)

        # print "len ACTIONS", len(ACTIONS)
        # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=index_name, raise_on_error=True)
        print('Performed %d actions' % success)

# 读取参数
def read_args():
    parser = argparse.ArgumentParser(description="Search Elastic Engine")
    parser.add_argument("-i", dest="input_file", action="store", help="input file1", required=False, default="./data.txt")
    # parser.add_argument("-o", dest="output_file", action="store", help="output file", required=True)
    return parser.parse_args()

# 初始化es，设置mapping
def init_es(hosts=[], timeout=5000, index_name=INDEX_NAME, doc_type_name=TYPE_NAME):
    es = Elasticsearch(hosts, timeout=5000)
    my_mapping = {
        TYPE_NAME: {
            "properties": {
                "@timestamp": {
                    "type": "date",
                    "format": "epoch_second"
                },
                "time": {
                    "type": "integer"
                },
                "user&domain": {
                    "type": "keyword"
                },
                "source_computer": {
                    "type": "keyword"
                },
                "destination_computer": {
                    "type": "keyword"
                }
            }
        }
    }
    try:
        # 先销毁，后创建Index和mapping
        delete_index = es.indices.delete(index=index_name)  # {u'acknowledged': True}
        create_index = es.indices.create(index=index_name)  # {u'acknowledged': True}
        mapping_index = es.indices.put_mapping(index=index_name, doc_type=doc_type_name,
                                                    body=my_mapping)  # {u'acknowledged': True}
        if delete_index["acknowledged"] != True or create_index["acknowledged"] != True or mapping_index["acknowledged"] != True:
            print("Index creation failed...")
    except (Exception):
        print ("set_mapping except")


# 主函数
if __name__ == '__main__':
    # args = read_args()
    # 初始化es环境
    init_es(hosts=["localhost:9200"], timeout=5000)
    # 创建es类
    es = es_tool(hosts=["localhost:9200"], timeout=5000)
    # 执行写入操作
    batchsize=50
    i=0
    datalist=[]
    with open("redteam.txt") as f:
        while i<batchsize:
            i+=1

            line=f.readline()
            if not line:
                if len(datalist)!=0:
                    es.set_data(datalist)
                break
            datalist.append(analyze(line))
            if(i==batchsize):
                # print("datalist",datalist)
                es.set_data(datalist)
                datalist = []
                i=0


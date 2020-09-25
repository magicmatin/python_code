from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import re
import time
import json

#数据源topic
topic = "test"
#转换topic
transformer_topic = 't_test'
#agent数据转换
agent_list = ['iOS', 'Android']


def send_transform_data(producer,send_data):
    """
    发送转换的数据
    :return:
    """

    # 获取user_id保证同一个user_id发送给同一个分区
    user_id = send_data.get("user_id")
    #发送数据
    feature = producer.send(transformer_topic,value=send_data,key=user_id)
    try:
        record_metadata = feature.get(timeout=10)
        print(record_metadata)
    except KafkaError as e:
        #日志记录
        print(e)


def dict_pop_key(dict_data,key):
    """
    移除dict某个key
    :param dict_data:
    :param key:
    :return:
    """
    if key in dict_data.keys():
        dict_data.pop(key)

def convert_timestamp(date_str):
    """
    时间字符串转时间戳
    :param date_str:
    :return:
    """
    # 转为时间数组
    time_array = time.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    # 转为时间戳
    time_stamp = int(time.mktime(time_array) * 1000)
    return time_stamp

def is_number(num_str):
    pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*|[-+]?\.?[0-9]\d*$')
    result = pattern.match(num_str)
    if result:
        return True
    else:
        return False


def convert_data(raw_json):
    """
    数据转换
    :param raw_json: dict
    :return:
    """
    if isinstance(raw_json,dict):
        #转换字段获取
        version = raw_json.get("version")
        longtitude = raw_json.get("longtitude")
        latitude = raw_json.get("latitude")

        if longtitude and is_number(longtitude):
            longtitude = float(longtitude)
        else:
            longtitude = ''
        if latitude and is_number(latitude):
            latitude = float(latitude)
        else:
            latitude = ''
        #转换时间
        log_time = raw_json.get("log_time")
        if log_time:
            log_time = convert_timestamp(log_time)
        else:
            log_time = ''


        #转换位置
        location = dict()
        location['lat'] = latitude
        location['lng'] = longtitude

        #转换common
        app_type = raw_json.get("app_type")
        common_dict = dict()
        common_dict['version'] = version if version else ''
        if app_type==100:
            client = raw_json.get("client")
            if isinstance(client,dict):
                user_agent = client.get("user_agent")
                if user_agent in agent_list:
                    common_dict['platform'] = user_agent
                    common_dict['os'] = user_agent
                else:
                    common_dict['platform'] = 'H5'
                    common_dict['os'] = 'UNKNOWN'
                dict_pop_key(client,'user_agent')
            raw_json['client']=client

        #更新转换json
        dict_pop_key(raw_json,'longtitude')
        dict_pop_key(raw_json,'latitude')
        dict_pop_key(raw_json,'log_time')
        raw_json.update({"common":common_dict})
        raw_json.update({"location":location})
        raw_json.update({"timestampMs":log_time})

def consumer_data():
    """
    消费数据
    :return:
    """
    #消费源数据
    consumer = KafkaConsumer(
        topic,
        group_id="user-event-test",
        bootstrap_servers=[
            "localhost:9092"
        ],
        value_deserializer=lambda m:json.loads(m.decode('utf8'))
    )
    #转换数据生产者
    producer = KafkaProducer(
        bootstrap_servers=[
            'localhost:9092'
        ],
        value_serializer=lambda m: json.dumps(m).encode('utf8'),
        key_serializer=lambda m: m.encode("utf8")
    )

    while True:
        try:
            #拉取数据
            batch_msg = consumer.poll(timeout_ms=5)
            if batch_msg:
                for _,msg_list in batch_msg.items():
                    for msg in msg_list:
                        value = msg.value
                        #数据转换
                        convert_data(value)
                        #发送转换数据
                        send_transform_data(producer,value)
        except KafkaError as e:
            # 日志记录
            print(e)




if __name__ == '__main__':
    consumer_data()








## hwkafka2kudu使用说明



#### 一 . 提供impala jdbc 和 kudu client 两种方式

##### 要求

- 要求输入的java对象的字段与sink table的字段保持一致,包括数量
    - 正确 : 要求目标表字段全部大写或者全部小写
        - java字段 a , 表字段 a/A
        - java字段 abc , 表字段 abc/ABC
        - java字段 aBc , 表字段 abc/ABC
    - 错误 : 目标表有大小写混写
        - java字段 abc 表字段 aBc
- 如果表字段有大小写混写,则java对象字段名要和表字段名保持一致
- 需要表的主键,没有主键无法完成update和delete
- 代码实现由反射实现,如果要sink自己的表,则要创建好对象的java实体类

#### 二. 配置文件说明

> 配置文件请全部都指定,如果没有的则给一个默认值,在程序中没有给默认值

```conf
# kafka
bootstrap.servers="xxx:9092,xxx:9092,xxx:9092"
key.serializer="org.apache.kafka.common.serialization.StringSerializer"
key.deserializer="org.apache.kafka.common.serialization.StringDeserializer"
kafka.source.topic="test"
group.id="test"
# kerberos
# jaas文件和krb5的配置文件
kafka.jaas="/xxx/jaas.conf"
kafka.krb5="/xxx/krb5.conf"

# impala & kudu
kudu.sink.table="xxx"
kudu.master.address="ip:port,ip:port,ip:port"
impala.ip="ip"
impala.port="port"
impala.db="default"
impala.username="xxx"
impala.password="xxx"
# 如果自己直接指定了url,则用指定的url否则将自动拼接
impala.url=""
# impala ldap
impala.uid="xxx"
impala.pwd="xxx"
# 是否开启ldap认证,如果开启,则会拼接相关参数
impala.ldap=false

# batch
# pk为主键,或者联合主键,必须指定
kudu.pk="acct_no,rec_no,post_date"
# 是否为batch模式
sink.batch.mode=true
# batch批次大小
sink.batch.size=1024

# other
# java对象字段名是否和表字段名对应
filed.upper=true
```




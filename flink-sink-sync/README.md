#### 同步工具设计思想

##### 1.反序列化设计思想：时区纠正及脱敏

Java序列化是指把Java对象转换为字节序列的过程；而Java反序列化是指把字节序列恢复为Java对象的过程。 构建自定义反序列化类，实现DebeziumDeserializationSchema接口，覆写deserialize方法，将SourceRecord的binlog进行拆分，分为before,after,source,op等json块，重新组装成JSONObject对象，jsonObject.toJSONString后放入收集器collector。 在反序列化器中，进行一些自定义转换：

①时间字段的按时时区显示（UTC->Asia/Shanghai） 

②脱敏转换（对隐私信息进行预处理【比如去除空格和“-”】后正则匹配，对不同的匹配类型用不同的脱敏方式）

 转换实现方式：  after.schema().fields()遍历出field（列），after.get(field)为列值，对其转换。  通过创建自定义时间转换方法，返回一个将convert覆写的DeserializationRuntimeConverter接口对象。在覆写 的convert内实现转换。

 通过判断field的数据类型field.schema()的type和name，判断是否为时间相关的列，以及它是哪种时间类型，进而相应的转换方法。  通过判断[field.name](http://field.name)列名为脱敏关键字，以及source表名是否为脱敏表，列数据类型为String等，对其进行脱敏转换。

##### 2.DataX脱敏设计思想

在datax.core.transformer下新建转换类MaskingTransformer，并在TransformerRegistry中添加其注册。 MaskingTransformer继承抽象类transformer，覆写evolute评估方法，对transformerJson入参数量和类型进行判断(参数一：列序号；参数二：脱敏形式)。

 符合条件的record.setColumn(columnIndex,newStringColumn(MaskingUtil.mask(oriValue,maskMode)))。

开发实现遍历TableList自动生成dataxJson文件并执行批同步的功能。

##### 3.元数据复用思想

 解构mysql DDL，将各种元数据信息装载到Map<String, Map>字典中； 

 元数据信息主要包括：表库信息、目标表库信息、连接系列配置信息、primaryKey、建表语句、列信息以及衍生的各种SQL的with配置等其他tail信息。

##### 4.安全的保存点设计

 为保障实时同步任务的数据安全可靠，已在DbFullStreamingSyncDoris类中设计保存点配置，每个同步任务会默认将保存点指定在和尾缀为库名的地址，防止系统异常时难以找回。

#### 同步工具设计模块

##### 1.实时同步模块

主要利用FlinkCDC的scanNewlyAddedTableEnabled功能，实现可增可减的同步表。 在mysqlSource配置中，创建新的自定义反序列化器，实现时区纠正和脱敏。 非删除操作时不同的表分流写入不同侧输出流；删除操作时，按主键删除行。

##### 2.批量同步模块

创建自动生成datax json并执行的任务 为实现脱敏功能，在DataX源码中添加脱敏转换器（见DataX脱敏设计思想）。

##### 3.远程连接及配置模块

为适配不同部署平台对配置文件的读取，将配置远程化 配置文件及信息主要包括：TableList配置表、元数据管理表连接信息、元数据管理表、目标Kafka配置信息、目标Doris连接信息、脱敏配置等。

#### 涉及的功能类和工具类

##### 1.自定义SinkFunction

返回DorisSink对象和FlinkKafkaProducer对象

##### 2.自定义数据类型转换Function

实现对mysql到flink、doris的数据类型转换

##### 3.自定义反序列化器

##### 4.自定义DataX Job任务生成器

##### 5.HDFS远程化读写工具类

##### 6.SSH代理登录工具类

##### 7.Druid连接池工具类

##### 8.MysqlDDL解析工具类

##### 9.列名核验工具类

##### 10.TableList配置表封装工具类等

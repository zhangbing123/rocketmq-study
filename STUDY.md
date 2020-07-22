源码目录结构：

broker: broker 模块（broke 启动进程）

client ：消息客户端，包含消息生产者、消息消费者相关类

common ：公共包

dev ：开发者信息（非源代码）

distribution ：部署实例文件夹（非源代码）

example: RocketMQ 例代码

filter ：消息过滤相关基础类

filtersrv：消息过滤服务器实现相关类（Filter启动进程）

logappender：日志实现相关类

namesrv：NameServer实现相关类（NameServer启动进程）

openmessageing：消息开放标准

remoting：远程通信模块，给予Netty

srcutil：服务工具类

store：消息存储实现相关类

style：checkstyle相关实现

test：测试相关类

tools：工具类，监控命令相关实现类
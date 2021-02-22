| 命令                                                         | 说明                                          | 参数                                           |
| ------------------------------------------------------------ | --------------------------------------------- | ---------------------------------------------- |
| create [-s] [-e] path data all<br />若不加中括号里参数默认创建永久节点 | 创建Znode                                     | -s指定是顺序节点<br />-e指定是临时节点         |
| ls path [watch]                                              | 列出Path下所有子Znode                         |                                                |
| get path [watch]                                             | 获取Path对应的Znode的数据和属性               |                                                |
| ls2 path [watch]                                             | 查看Path下左右子Znode以及子Znode的属性        |                                                |
| set path data [version]                                      | 更新节点                                      | version数据版本                                |
| delete path [version]                                        | 删除节点，如果要删除的节点有子Znode则无法删除 | version数据版本                                |
| rmr path                                                     | 删除节点，如果有子Znode则递归删除             |                                                |
| setquota -n \| -b val path                                   | 修改Znode配额                                 | -n设置子节点最大个数<br />-b设置子节点最大长度 |
| history                                                      | 列出历史记录                                  |                                                |

path一般为路径名

data为数据（参数内容）

acl为权限控制

##### 操作实例

- 列出Path下的所有Znode
- - ls /
- 创建永久节点
- - create /hello world
- 创建临时节点
- - create -e /abc 123
- 创建永久序列化节点
- - create -s /zhangsan boy
- 创建临时序列化节点
- - create -e -s /lisi boy
- 修改节点数据
- - set /hello zookeeper
- 删除节点，如果要删除的节点有子Znode则无法删除
- - delete /hello
- 删除节点，如果有子Znode则递归删除
- - rmr /abc
- 列出历史记录
- - history
- 获取节点属性
- - get -s /hello
  - dataVersion：数据版本号，每次对节点进行set操作，dataVersion的值都会增加1（即使设置的值时相同的数据），可有效避免数据更新时出现的先后顺序问题
  - cversion：子节点的版本号，当Znode子节点有变化时，cversion的值会增加1
  - aclVersion：ACL的版本号
  - cZxid：Znode创建的事务id
  - mZxid：Znode被修改的事务id
  - - 对于zk来说，每次的变化都会产生一个唯一的事务id：zxid。通过zxid，可以确定操作的先后顺序
    - 如果zxid1小于zxid2，说明zxid1操作先于zxid2
  - ctime：节点创建的时间戳
  - mtime：节点更新一次发生时的时间戳
  - ephemeralOwner：如果该节点为临时节点，ephemeralOwner值表示与该节点绑定的session id，如果不是，则ephemeralOwner值为0
- 

##### Zookeeper的watch机制

- 类似于触发器，当Znode发生变化时，WatchManager会调用对应的Watcher
- 当Znode发生删除，修改，创建，子节点时，对应的Watcher会得到通知
- Watcher 的特点
- - 一次性触发一个watcher，只能被触发一次，如果需要再次监听，则需要重新添加一个watcher
  - 事件封装：Watcher得到的事件是被封装过的，包括三个内容keeperState，eventType，path
- 

| KeeperState   | EventType        | 触发条件                 | 说明                               |
| ------------- | ---------------- | ------------------------ | ---------------------------------- |
|               | None             | 连接成功                 |                                    |
| SyncConnected | NodeCreated      | Znode被创建              | 此时处于连接状态                   |
| SyncConnected | NodeDeleted      | Znode被删除              | 此时处于连接状态                   |
| SyncConnected | NodeDataChanged  | Znode数据被改变          | 此时处于连接状态                   |
| SyncConnected | NodeChildChanged | Znode的子Znode数据被改变 | 此时处于连接状态                   |
| Disconnected  | Node             | 客户端与服务端断开连接   | 此时客户端与服务端处于断开连接状态 |
| Expired       | None             | 会话超时                 | 会收到一个SesionExpiredException   |
| AuthFailed    | None             | 权限验证失败             | 会收到一个AuthFailedException      |







##




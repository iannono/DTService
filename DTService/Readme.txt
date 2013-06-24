**各目录中文对照表**

*pincome: 客运销售收入贡献
*cincome:常客销售收入
*cargoincome:货邮收入
*et:et
*flightplan:航班计划销售贡献指标
*groupincome:大客户销售收入
*hubincome:枢纽中转销售收入
*lineincome:BO航线座公里收入汇总
*fltincome: fltincome

**配置选项**

*FilePath:用于标识数据表存放的主目录，其下按照中文对照表建设各表目录
*FtpPath:用于标识Ftp文件的存放主目录，其下存放fltincome目录文件

**数据库相关**

*et数据表因为需要进行更新操作，而且一个文件可能含有多天的记录，所以需要一张额外的临时表来存储导入数据，名称为et_temp

*pincome、hubincome两张表对应的文件，需要文件名以类似'201205'的形式开头，用于标明当前数据文件是基于哪个月份的;
这样是为了避免导入的时候，数据和文件不符，导致误删除数据的情况发生。
如果需要导入历史数据，即一个文件包含过去几个月的数据，只要数据的第一条月份与文件名称保持一直即可，可以采用"201306-201301"这样的命名方式来标识文件内容。

*fltincome默认是没有表头标题的

*sfincome中对联程的处理，是按照3条联程数据连在一起的逻辑处理的

*cargoincome的文件名需要采用类似'20130608'的形式开头

**webservice**

*系统提供了基于HTTP的API，通过向http://localhost/api/dataservice/tablename形式的地址发起get请求，来执行对应的导入方法，返回执行结果



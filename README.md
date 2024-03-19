# gopher-database

gopher-database是gopher的数据库扩展组件，用于集成数据库相关操作。

内置ORM工具为[gobatis](https://github.com/ydx1011/gobatis)

## 安装
```
go get github.com/ydx1011/gopher-database
```

## 使用

### 1. gopher集成（依赖[gopher-core](https://github.com/ydx1011/gopher-core)）
```
app := gopher.NewFileConfigApplication("assets/config-example.yaml")
app.RegisterBean(gobatiseve.NewProcessor())

//Register other beans...
//app.RegisterBean(test.NewMyTest())
app.Run()
```

### 2. 配置
在config-example.yaml中配置示例如下：
```
gopher:
  dataSources:
    testDB:
      driverName: "mysql"
      driverInfo: "test:123@tcp(127.0.0.1:3306)/test?timeout=10s&readTimeout=15s&charset=uft8"
      maxConn: 1000
      maxIdleConn: 500
      connMaxLifetime: 15

gobatis:
  mapper:
    dir: "assets/mappers"
  log:
    level: DEBUG
  pagehelper:
    enable: true
```
* 【gopher.dataSources.testDB】为注入的DataSource名称，请根据实际项目进行修改，通过inject注入名称为testDB可直接获得SessionManager（testDB下为数据库相关配置）。

  (SessionManager用法参考[gobatis](https://github.com/ydx1011/gobatis)及[gobatis-cmd](https://github.com/ydx1011/gobatis-cmd))
```
type MyTest struct {
	SessMgr *gobatis.SessionManager `inject:"testDB"`
} 

func (t *MyTest) test() {
	sess := t.SessMgr.NewSession()
	var ret []test
	sess.Select("select * from test").Param().Result(&ret)
}
```
* 【gobatis.mapper.dir】配置扫描mapper文件的路径
* 【gobatis.mapper.log】配置日志级别，默认为info
* 【gobatis.mapper.pagehelper】配置是否启用自动分页（集成分页插件）

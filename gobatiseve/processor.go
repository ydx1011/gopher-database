package gobatiseve

import (
	"errors"
	"github.com/xfali/xlog"
	"github.com/ydx1011/gobatis"
	"github.com/ydx1011/gobatis/datasource"
	"github.com/ydx1011/gobatis/factory"
	"github.com/ydx1011/gopher-core"
	"github.com/ydx1011/gopher-core/bean"
	"github.com/ydx1011/yfig"
	"strings"
	"sync"
	"time"
)

const (
	BuildinValueDataSources = "gopher.dataSources"
)

type DataSource struct {
	DriverName string
	DriverInfo string

	MaxConn     int
	MaxIdleConn int
	//millisecond
	ConnMaxLifetime int
}

type FactoryCreatorWrapper func(f func(source *DataSource) (factory.Factory, error)) func(source *DataSource) (factory.Factory, error)

type Processor struct {
	logger        xlog.Logger
	facWrapper    FactoryCreatorWrapper
	dataSources   sync.Map
	usePageHelper bool
	gobatisLog    string
}

type Opt func(*Processor)

func NewProcessor(opts ...Opt) *Processor {
	ret := &Processor{
		logger:     xlog.GetLogger(),
		facWrapper: defaultWrapper,
	}

	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (p *Processor) Init(conf yfig.Properties, container bean.Container) error {
	dss := map[string]*DataSource{}
	err := conf.GetValue(BuildinValueDataSources, &dss)
	if err != nil {
		return err
	}
	if len(dss) == 0 {
		p.logger.Errorln("No Database")
		return nil
	}
	p.usePageHelper = conf.Get("gobatis.pagehelper.enable", "") == "true"
	p.gobatisLog = strings.ToUpper(conf.Get("gobatis.log.level", "DEBUG"))

	for k, v := range dss {
		fac, err := p.facWrapper(p.createFactory)(v)
		if err != nil {
			p.logger.Errorln("init db failed")
			return err
		}
		sm := gobatis.NewSessionManager(fac)
		p.dataSources.Store(k, sm)
		//添加到注入容器
		container.RegisterByName(k, sm)
	}

	// scan mapper files
	mapper := conf.Get("gobatis.mapper.dir", "")
	if mapper != "" {
		return gobatis.ScanMapperFile(gopher.GetResource(mapper))
	}
	return nil
}

func (p *Processor) createFactory(v *DataSource) (factory.Factory, error) {
	fac, err := gobatis.CreateFactory(
		gobatis.SetMaxConn(v.MaxConn),
		gobatis.SetMaxIdleConn(v.MaxIdleConn),
		gobatis.SetConnMaxLifetime(time.Duration(v.ConnMaxLifetime)*time.Millisecond),
		gobatis.SetLog(p.selectLog()),
		gobatis.SetDataSource(&datasource.CommonDataSource{
			Name: v.DriverName,
			Info: v.DriverInfo,
		}))
	if fac == nil || err != nil {
		return nil, err
	}
	if p.usePageHelper {
		//fac = pagehelper.New(fac)
	}
	return fac, err
}

func (p *Processor) Classify(o interface{}) (bool, error) {
	switch v := o.(type) {
	case Component:
		err := p.parseBean(v)
		return true, err
	}
	return false, nil
}

func (p *Processor) parseBean(comp Component) error {
	name := comp.DataSource()
	if v, ok := p.dataSources.Load(name); ok {
		comp.SetSessionManager(v.(*gobatis.SessionManager))
	}
	p.logger.Errorln("DataSource Name found: ", name)
	return errors.New("DataSource Name found. ")
}

func (p *Processor) Process() error {
	return nil
}

func (p *Processor) BeanDestroy() error {
	p.dataSources.Range(func(key, value interface{}) bool {
		value.(*gobatis.SessionManager).Close()
		return true
	})
	return nil
}

func defaultWrapper(f func(source *DataSource) (factory.Factory, error)) func(source *DataSource) (factory.Factory, error) {
	return f
}

func (p *Processor) selectLog() func(level int, fmt string, o ...interface{}) {
	switch p.gobatisLog {
	case "DEBUG":
		return func(level int, fmt string, o ...interface{}) {
			xlog.Debugf(fmt, o...)
		}
	case "INFO":
		return func(level int, fmt string, o ...interface{}) {
			xlog.Infof(fmt, o...)
		}
	case "WARN":
		return func(level int, fmt string, o ...interface{}) {
			xlog.Warnf(fmt, o...)
		}
	case "ERROR":
		return func(level int, fmt string, o ...interface{}) {
			xlog.Errorf(fmt, o...)
		}
	}
	return func(level int, fmt string, o ...interface{}) {
		xlog.Debugf(fmt, o...)
	}
}

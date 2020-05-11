package sql

import (
	"context"
	"github.com/go-pg/pg/v9"
	"github.com/xieqiaoyu/xin"
	"github.com/xieqiaoyu/xin/db/sql"
	xlog "github.com/xieqiaoyu/xin/log"
)

//pgConfig config support pg setting
type PgConfig interface {
	sql.Config
	EnableDbLog() bool
}
type PgWrap struct {
	DB      *pg.DB
	Options *pg.Options
}

//PgService PgService
type PgService struct {
	*sql.Service
	config PgConfig
}

func newPgEngineHandler(config PgConfig) sql.GenEngineFunc {
	return func(driverName, dataSourceName string) (engine interface{}, err error) {
		opt, err := pg.ParseURL(dataSourceName)
		if err != nil {
			return nil, xin.NewTracedEf("Fail to new pg database source [%s], Err:%w", dataSourceName, err)
		}
		db := pg.Connect(opt)
		if config.EnableDbLog() {
			db.AddQueryHook(pgLogger{})
		}
		w := &PgWrap{
			DB:      db,
			Options: opt,
		}
		return w, nil
	}
}

func closePgEngine(engine interface{}) error {
	w, ok := engine.(*PgWrap)
	if !ok {
		return xin.NewTracedEf("engine is not a *PgWrap")
	}
	return w.DB.Close()
}

//NewPgService NewPgService
func NewPgService(config PgConfig) *PgService {
	return &PgService{
		config:  config,
		Service: sql.NewService(config, newPgEngineHandler(config), closePgEngine),
	}
}

func (s *PgService) GetWrap(id string) (*PgWrap, error) {
	w, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	wrap, ok := w.(*PgWrap)
	if !ok {
		return nil, xin.NewTracedEf("db id %s is not a *PgWrap", id)
	}
	return wrap, nil
}

//Engine load an pg engine by id
func (s *PgService) Engine(id string) (engine *pg.DB, err error) {
	wrap, err := s.GetWrap(id)
	if err != nil {
		return nil, err
	}
	return wrap.DB, nil
}

//Session  load conn by id ,should close session after everything is done
func (s *PgService) Session(id string) (session *pg.Conn, err error) {
	engine, err := s.Engine(id)
	if err != nil {
		return nil, err
	}
	return engine.Conn(), nil
}

type pgLogger struct{}

func (l pgLogger) BeforeQuery(c context.Context, q *pg.QueryEvent) (context.Context, error) {
	return c, nil
}

func (l pgLogger) AfterQuery(c context.Context, q *pg.QueryEvent) error {
	xlog.Debugf(q.FormattedQuery())
	return nil
}

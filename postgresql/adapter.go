package postgresql

import (
	"context"
	"fmt"
	instana "github.com/instana/go-sensor"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	ot "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"net/http"
)

type Pool struct {
	sensor *instana.Sensor
	tracer ot.Tracer
	config *pgxpool.Config
	pool   *pgxpool.Pool
}

func NewWithConnectionString(sensor *instana.Sensor, connectionString string, ctx context.Context) (*Pool, error) {
	var tracer ot.Tracer
	sensor.WithTracer(func(t ot.Tracer) {
		tracer = t
	})

	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return &Pool{
		sensor: sensor,
		tracer: tracer,
		config: config,
		pool:   pool,
	}, nil
}

func (p *Pool) Query(req *http.Request, ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	parentSpan := req.Context().Value("parentSpan")

	if ps, ok := parentSpan.(ot.Span); ok {
		return p.QueryWithParentSpan(ps, ctx, query, args...)
	}
	return p.QueryWithSpan(nil, ctx, query, args...)
}

func (p *Pool) QueryWithParentSpan(parentSpan ot.Span, ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	var span ot.Span
	if parentSpan != nil {
		span = p.tracer.StartSpan(
			query,
			ot.ChildOf(parentSpan.Context()),
		)
	} else {
		span = p.tracer.StartSpan(
			query,
		)
	}
	return p.QueryWithSpan(span, ctx, query, args...)
}

func (p *Pool) QueryWithSpan(span ot.Span, ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	host := p.config.ConnConfig.Host
	port := p.config.ConnConfig.Port
	user := p.config.ConnConfig.User
	db := p.config.ConnConfig.Database

	span.SetTag(string(ext.SpanKind), string(ext.SpanKindRPCClientEnum))
	//span.SetOperationName("postgres")
	span.SetTag(string(ext.DBType), "postgresql")
	span.SetTag(string(ext.DBInstance), db)
	span.SetTag(string(ext.DBUser), user)
	span.SetTag(string(ext.DBStatement), query)
	span.SetTag(string(ext.PeerAddress), fmt.Sprintf("%s:%d", host, port))
	/*p.sensor.SetSpanAttribute(span, instana.PostgreSQL,
		fmt.Sprintf(`{"stmt":"%s","host":"%s","port":%d,"user":"%s","db":"%s"}`, query, host, port, user, db),
	)*/
	defer span.Finish()

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		span.SetTag(string(ext.Error), err.Error())
	}
	return rows, err
}

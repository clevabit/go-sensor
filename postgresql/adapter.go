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
		tracer: tracer,
		config: config,
		pool:   pool,
	}, nil
}

func (p *Pool) Query(req *http.Request, ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	parentSpan := req.Context().Value("parentSpan")

	var span ot.Span
	if ps, ok := parentSpan.(ot.Span); ok {
		span = p.tracer.StartSpan(
			query,
			ot.ChildOf(ps.Context()),
		)
	} else {
		span = p.tracer.StartSpan(
			query,
		)
	}

	span.SetTag(string(ext.SpanKind), string(ext.SpanKindRPCClientEnum))
	span.SetTag(string(ext.DBType), "postgres")
	span.SetTag(string(ext.DBInstance), fmt.Sprintf("%s:%d", p.config.ConnConfig.Host, p.config.ConnConfig.Port))
	span.SetTag(string(ext.DBUser), p.config.ConnConfig.User)
	span.SetTag(string(ext.DBStatement), query)
	defer span.Finish()

	return p.pool.Query(ctx, query, args...)
}

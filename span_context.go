// (c) Copyright IBM Corp. 2021
// (c) Copyright Instana Inc. 2020

package instana

import (
	"github.com/instana/go-sensor/w3ctrace"
)

// EUMCorrelationData represents the data sent by the Instana End-User Monitoring script
// integrated into frontend
type EUMCorrelationData struct {
	Type string
	ID   string
}

// SpanContext holds the basic Span metadata.
type SpanContext struct {
	// The higher 4 bytes of a 128-bit trace ID
	TraceIDHi int64
	// A probabilistically unique identifier for a [multi-span] trace.
	TraceID int64
	// A probabilistically unique identifier for a span.
	SpanID int64
	// An optional parent span ID, 0 if this is the root span context.
	ParentID int64
	// Whether the trace is sampled.
	Sampled bool
	// Whether the trace is suppressed and should not be sent to the agent.
	Suppressed bool
	// The span's associated baggage.
	Baggage map[string]string // initialized on first use
	// The W3C trace context
	W3CContext w3ctrace.Context
	// Correlation is the correlation data sent by the frontend EUM script
	Correlation EUMCorrelationData
}

// NewRootSpanContext initializes a new root span context issuing a new trace ID
func NewRootSpanContext() SpanContext {
	spanID := randomID()

	c := SpanContext{
		TraceID: spanID,
		SpanID:  spanID,
	}

	c.W3CContext = w3ctrace.New(w3ctrace.Parent{
		Version:  w3ctrace.Version_Max,
		TraceID:  FormatLongID(c.TraceIDHi, c.TraceID),
		ParentID: FormatID(c.SpanID),
	})

	return c
}

// NewSpanContext initializes a new child span context from its parent. It will
// ignore the parent context if it contains neither Instana trace and span IDs
// nor a W3C trace context
func NewSpanContext(parent SpanContext) SpanContext {
	if parent.TraceIDHi == 0 && parent.TraceID == 0 && parent.SpanID == 0 {
		parent = restoreFromW3CTraceContext(parent.W3CContext)
	}

	if parent.TraceIDHi == 0 && parent.TraceID == 0 && parent.SpanID == 0 {
		return NewRootSpanContext()
	}

	c := parent.Clone()
	c.SpanID, c.ParentID = randomID(), parent.SpanID

	return c
}

func restoreFromW3CTraceContext(trCtx w3ctrace.Context) SpanContext {
	if trCtx.IsZero() {
		return SpanContext{}
	}

	parent := trCtx.Parent()

	traceIDHi, traceIDLo, err := ParseLongID(parent.TraceID)
	if err != nil {
		return SpanContext{}
	}

	parentID, err := ParseID(parent.ParentID)
	if err != nil {
		return SpanContext{}
	}

	return SpanContext{
		TraceIDHi:  traceIDHi,
		TraceID:    traceIDLo,
		SpanID:     parentID,
		Suppressed: !parent.Flags.Sampled,
		W3CContext: trCtx,
	}
}

// ForeachBaggageItem belongs to the opentracing.SpanContext interface
func (c SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range c.Baggage {
		if !handler(k, v) {
			break
		}
	}
}

// WithBaggageItem returns an entirely new SpanContext with the
// given key:value baggage pair set.
func (c SpanContext) WithBaggageItem(key, val string) SpanContext {
	res := c.Clone()

	if res.Baggage == nil {
		res.Baggage = make(map[string]string, 1)
	}
	res.Baggage[key] = val

	return res
}

// Clone returns a deep copy of a SpanContext
func (c SpanContext) Clone() SpanContext {
	res := SpanContext{
		TraceIDHi:  c.TraceIDHi,
		TraceID:    c.TraceID,
		SpanID:     c.SpanID,
		ParentID:   c.ParentID,
		Sampled:    c.Sampled,
		Suppressed: c.Suppressed,
		W3CContext: c.W3CContext,
	}

	if c.Baggage != nil {
		res.Baggage = make(map[string]string, len(c.Baggage))
		for k, v := range c.Baggage {
			res.Baggage[k] = v
		}
	}

	return res
}

package sinkpostgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pool writes batches with COPY over a pgx pool.
type pool struct{ p *pgxpool.Pool }

func openPool(s *settings) (writer, error) {
	p, err := pgxpool.NewWithConfig(context.Background(), s.pool)
	if err != nil {
		return nil, err
	}
	return &pool{p: p}, nil
}

// Write sends one batch as one COPY, which the server applies whole or not
// at all.
func (p *pool) Write(ctx context.Context, r route, rows [][]any) error {
	_, err := p.p.CopyFrom(ctx, r.ident, columns, pgx.CopyFromRows(rows))
	return err
}

func (p *pool) Close() { p.p.Close() }

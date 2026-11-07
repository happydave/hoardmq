package authsql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pool is the lookup over a pgx pool.
type pool struct{ p *pgxpool.Pool }

func openPool(s *settings) (lookup, error) {
	p, err := pgxpool.NewWithConfig(context.Background(), s.pool)
	if err != nil {
		return nil, err
	}
	return &pool{p: p}, nil
}

// Query reads at most two rows, since two already refuse. The first column
// must be text; the others are read as their text form.
func (p *pool) Query(ctx context.Context, sql string, args []any) ([]string, [][]*string, error) {
	rows, err := p.p.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cols []string
	for _, f := range rows.FieldDescriptions() {
		cols = append(cols, f.Name)
	}
	var out [][]*string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		row := make([]*string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
			case string:
				row[i] = &x
			case []byte:
				s := string(x)
				row[i] = &s
			default:
				if i == 0 {
					return nil, nil, fmt.Errorf("the query's first column is %T, not text", v)
				}
				s := fmt.Sprint(x)
				row[i] = &s
			}
		}
		out = append(out, row)
		if len(out) == 2 {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, out, nil
}

func (p *pool) Close() { p.p.Close() }

# GQL — GojoDB Query Language Reference

GQL is GojoDB's built-in query language. It is case-insensitive, whitespace-insensitive, and is parsed by a hand-written recursive-descent parser (`core/query_engine/parser`). Parsed statements are passed through the rule-based optimizer (`core/query_engine/optimizer`) and then executed by the plan executor (`core/query_engine/executor`).

---

## Table of Contents

1. [Using GQL](#using-gql)
2. [Lexical Rules](#lexical-rules)
3. [Statements](#statements)
   - [SELECT](#select)
   - [INSERT](#insert)
   - [DELETE](#delete)
   - [UPDATE](#update)
4. [Predicates (WHERE clause)](#predicates-where-clause)
   - [Equality](#equality--eq)
   - [Comparison](#comparison-operators)
   - [Range](#range--between--and-)
   - [Boolean combinators](#boolean-combinators)
   - [Full-text search — CONTAINS](#full-text-search--containsfield-query)
   - [Spatial proximity — NEAR](#spatial-proximity--nearlat-lon-radius)
   - [Spatial bounding box — WITHIN](#spatial-bounding-box--withinminlat-minlon-maxlat-maxlon)
5. [Clauses](#clauses)
   - [ORDER BY](#order-by)
   - [LIMIT](#limit)
6. [Result format](#result-format)
7. [Query execution pipeline](#query-execution-pipeline)
8. [Index selection rules](#index-selection-rules)
9. [Limitations and future work](#limitations-and-future-work)

---

## Using GQL

**From Go code** — use the convenience wrapper that parses, optimises, and executes in one call:

```go
result := executor.ExecuteQuery(ctx, `SELECT * FROM users WHERE key = 'user:42'`)
if result.Error != nil { ... }
for _, row := range result.Rows {
    fmt.Println(row.Key, row.Value)
}
```

**Step by step** — useful when you want to inspect or reuse the plan:

```go
stmt, err := parser.Parse(`SELECT * FROM logs WHERE CONTAINS(body, 'error')`)
plan, err := optimizer.Optimize(stmt)
result := exec.Execute(ctx, plan)
```

---

## Lexical Rules

| Category | Rules |
|----------|-------|
| **Keywords** | Case-insensitive. `SELECT`, `INSERT`, `INTO`, `UPDATE`, `SET`, `DELETE`, `FROM`, `WHERE`, `AND`, `OR`, `NOT`, `BETWEEN`, `ORDER`, `BY`, `ASC`, `DESC`, `LIMIT`, `VALUES`, `CONTAINS`, `NEAR`, `WITHIN` |
| **Identifiers** | Letters, digits, `_`. Must start with a letter or `_`. Used as index names and column names. |
| **String literals** | Enclosed in single quotes: `'hello'`. To include a literal single quote, double it: `'it''s'` → `it's`. |
| **Number literals** | Integer or decimal: `42`, `-3`, `37.7749`. Negative numbers supported (leading `-`). |
| **Operators** | `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=` |
| **Punctuation** | `(`, `)`, `,`, `*` |
| **Whitespace** | Ignored between tokens. |

---

## Statements

### SELECT

```
SELECT ( * | col [, col ...] )
FROM   <index>
[ WHERE <predicate> ]
[ ORDER BY <field> [ ASC | DESC ] ]
[ LIMIT <n> ]
```

**Returns** zero or more rows of `(key, value)` pairs.

```sql
-- All rows (full scan, up to internal default of 10 000)
SELECT * FROM products

-- Single row — point lookup
SELECT * FROM products WHERE key = 'sku:001'

-- Range scan with sort and pagination
SELECT * FROM orders WHERE key BETWEEN 'order:100' AND 'order:200' ORDER BY key ASC LIMIT 50

-- Full-text search
SELECT * FROM articles WHERE CONTAINS(body, 'distributed systems')

-- Spatial proximity search (radius in metres)
SELECT * FROM locations WHERE NEAR(51.5074, -0.1278, 1000)

-- Spatial bounding box
SELECT * FROM locations WHERE WITHIN(51.4, -0.2, 51.6, -0.1)
```

**Column list** — currently `*` is idiomatic; named columns are parsed but the executor always returns both `key` and `value`.

---

### INSERT

```
INSERT INTO <index> (key, value) VALUES ('<key>', '<value>')
```

- Both key and value must be single-quoted string literals.
- The column label names `key` and `value` are positional; any identifier is accepted.
- Returns `RowsAffected = 1` on success.

```sql
INSERT INTO users (key, value) VALUES ('user:42', '{"name":"Alice","age":30}')
INSERT INTO logs  (key, value) VALUES ('log:2026-05-07T12:00:00Z', 'server started')
```

---

### DELETE

```
DELETE FROM <index> WHERE <predicate>
```

- Predicate must resolve to a key expression that the optimizer can push to the B-tree.
- If the predicate is an exact equality (`key = 'x'`), the delete is a direct B-tree point delete.
- For range or compound predicates, the executor first performs a scan, then deletes each matching key.
- Returns `RowsAffected` equal to the number of keys deleted.

```sql
DELETE FROM sessions WHERE key = 'session:abc123'

-- Range delete (scan then delete each key)
DELETE FROM logs WHERE key BETWEEN 'log:2024' AND 'log:2025'
```

---

### UPDATE

```
UPDATE <index> SET value = '<new_value>' WHERE <predicate>
```

- Only the `value` column can be updated; `key` is immutable (delete + insert to rename a key).
- The `SET` clause currently accepts only `value = '<string>'`.
- Point update (equality predicate) is a direct overwrite.
- Range update applies the new value to every matching key.

```sql
UPDATE users SET value = '{"name":"Alice","age":31}' WHERE key = 'user:42'

-- Rewrite all values in a range
UPDATE config SET value = 'disabled' WHERE key BETWEEN 'feature:a' AND 'feature:z'
```

---

## Predicates (WHERE clause)

Predicates may appear in `SELECT`, `DELETE`, and `UPDATE` statements.

### Equality — `= eq`

```sql
WHERE key = 'some_key'
```

Mapped to a **B-tree point lookup** by the optimizer. O(log n) cost.

---

### Comparison operators

```sql
WHERE score >= '90'
WHERE created_at < '2025-01-01'
WHERE status != 'deleted'
WHERE version <> '1'
```

Supported operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`.

Comparison predicates that cannot be absorbed as a B-tree range scan are evaluated as a **residual filter** over a full scan. Currently the optimizer absorbs `>=` and `<=` as a half-open range scan when the field is `key`.

---

### Range — `BETWEEN … AND …`

```sql
WHERE key BETWEEN 'a' AND 'z'
WHERE key BETWEEN 'order:1000' AND 'order:9999'
```

Mapped to a **B-tree range scan**. Both bounds are inclusive. O(k + log n) where k is the number of matching keys.

---

### Boolean combinators

```sql
WHERE key = 'x' AND status != 'deleted'
WHERE CONTAINS(title, 'news') OR CONTAINS(body, 'news')
WHERE NOT key = 'reserved'
```

Supported: `AND`, `OR`, `NOT`.

**AND optimization**: The optimizer tries to push the left branch to the index and keeps the right branch as a residual filter. For best performance, put the most selective index predicate on the left side of `AND`.

**OR / NOT**: Currently evaluated as a post-scan residual filter (no index push-down). Both sides of an `OR` must refer to the same index.

---

### Full-text search — `CONTAINS(field, 'query')`

```sql
WHERE CONTAINS(body, 'distributed database')
WHERE CONTAINS(title, 'release notes')
```

Routes the query to the **inverted index** manager. The query string is tokenised (whitespace split), and each token is looked up in the term dictionary. Results are returned ranked by relevance score (stored as the `value` column in the result row).

- `field` — any identifier; currently all fields are indexed together, so this is informational.
- `query` — one or more space-separated search terms.
- Default result limit: 1 000.

---

### Spatial proximity — `NEAR(lat, lon, radius)`

```sql
WHERE NEAR(37.7749, -122.4194, 5000)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `lat` | decimal | Latitude of the centre point (degrees) |
| `lon` | decimal | Longitude of the centre point (degrees) |
| `radius` | decimal | Search radius in **metres** |

**How it works:**

1. The optimizer converts the circle into a bounding box using the approximations:
   - $\Delta\text{lat} = \text{radius} / 111\,000$
   - $\Delta\text{lon} = \text{radius} / (111\,000 \times \cos(\text{lat}))$
2. An R-tree bounding-box query is performed against the spatial index.
3. The original `NEAR` predicate is kept as a **residual filter** so that results outside the exact circle radius are discarded.

The `value` column in result rows contains the distance (in the internal R-tree score format).

---

### Spatial bounding box — `WITHIN(minLat, minLon, maxLat, maxLon)`

```sql
WHERE WITHIN(51.4, -0.2, 51.6, -0.1)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `minLat` | decimal | Southern boundary (degrees) |
| `minLon` | decimal | Western boundary (degrees) |
| `maxLat` | decimal | Northern boundary (degrees) |
| `maxLon` | decimal | Eastern boundary (degrees) |

Routes directly to the **R-tree spatial index** with no residual filter. All entries whose bounding rectangles intersect the query rectangle are returned.

---

## Clauses

### ORDER BY

```sql
ORDER BY <field> [ ASC | DESC ]
```

- `ASC` is the default.
- Applied after the scan; currently an **in-memory sort** over the result set.
- For B-tree range scans, `DESC` reverses the row list returned by `GetRange`.

```sql
SELECT * FROM events ORDER BY key DESC LIMIT 10   -- last 10 events
SELECT * FROM articles WHERE CONTAINS(body, 'db') ORDER BY key ASC LIMIT 20
```

---

### LIMIT

```sql
LIMIT <n>
```

- `n` must be a positive integer.
- Applied after all filters and sorting.
- If omitted: B-tree scans default to 10 000 rows; inverted / spatial scans default to 1 000 rows.

---

## Result format

Every query returns a `Result` struct:

```go
type Result struct {
    Rows         []Row   // SELECT results
    RowsAffected int64   // INSERT / DELETE / UPDATE count
    Error        error
}

type Row struct {
    Key   string
    Value string
}
```

For `INSERT`, `DELETE`, `UPDATE` — `Rows` is nil and `RowsAffected` carries the count.

For `CONTAINS` and spatial queries, `Value` contains a numeric score/distance as a string (e.g. `"0.920000"`).

---

## Query execution pipeline

```
Query string
    │
    ▼  parser.Parse()
  AST (Statement)
    │
    ▼  optimizer.Optimize()
  Physical Plan tree
    │
    ▼  executor.Execute()
  Result { Rows, RowsAffected, Error }
```

**Plan tree structure example** — `SELECT * FROM t WHERE key = 'x' AND CONTAINS(body, 'foo') LIMIT 5`:

```
Limit(5)
 └─ Filter(CONTAINS(body,'foo'))
     └─ BtreeScan(t, pointKey='x')
```

The B-tree absorbs the equality predicate; the full-text predicate becomes a residual filter; Limit caps the output.

---

## Index selection rules

The optimizer applies these rules in order:

| WHERE predicate | Index used | Plan node |
|-----------------|-----------|-----------|
| `key = 'x'` | B-tree | `BtreeScan` (point) |
| `key BETWEEN 'a' AND 'b'` | B-tree | `BtreeScan` (range) |
| `CONTAINS(field, 'text')` | Inverted index | `InvertedScan` |
| `NEAR(lat, lon, radius)` | Spatial R-tree | `SpatialScan` + residual `Filter` |
| `WITHIN(minLat, minLon, maxLat, maxLon)` | Spatial R-tree | `SpatialScan` |
| `pred AND pred` | Left branch pushed to index | residual `Filter` on right |
| `pred OR pred` / `NOT pred` | Full scan | residual `Filter` |
| No WHERE clause | B-tree | `BtreeScan` (full scan, up to 10 000) |

---

## Limitations and future work

| Area | Current state | Planned improvement |
|------|--------------|---------------------|
| Multi-index joins | Not supported | Hash/merge join plan nodes |
| Aggregations (`COUNT`, `SUM`, …) | Not supported | Aggregation plan node + push-down |
| Named column projections | Parsed but not enforced | Column projection in scan nodes |
| Cost-based optimisation | Rule-based only | Histogram statistics + cost model |
| `OR` index push-down | Full scan | Union of two index scans |
| Sub-queries | Not supported | Correlated and scalar sub-queries |
| `LIKE` / regex predicates | Not supported | Inverted index prefix/suffix support |
| Compound keys | Single string key only | Composite key encoding |

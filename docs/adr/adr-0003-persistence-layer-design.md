# ADR 0003: Persistence Layer Choice for DRS 
---
## Prior Art

See: See https://github.com/ga4gh/ga4gh-starter-kit-drs/blob/develop/database/postgresql/create-tables.sql

## What the schema tells us (important)

The DRS starter-kit schema has these defining traits:

### 1. **DB-first, SQL-authored**

* Canonical schema lives in SQL (`create-tables.sql`)
* No migration framework implied
* Tables are already normalized and intentional

### 2. **Protocol-driven, not CRUD**

Key tables:

* `drs_object`
* `file_access_object`
* `access_method`
* `bundle_object`
* `passport_visa`
* `drs_object_visa`

These exist to **implement GA4GH DRS semantics**, not generic CRUD entities.

### 3. **Authorization is relational**

* `drs_object_visa` is a join table
* Access decisions require **joins**, not object loading
* Queries like:

  > “Which access methods are visible given a passport?”

### 4. **Very little ORM-friendly behavior**

* No cascading business logic
* No polymorphic inheritance
* No lifecycle hooks
* Mostly read-heavy, join-heavy

This is *exactly* the kind of schema where “smart ORMs” become a liability.

---

## Best choice for THIS application

### ✅ **sqlc + pgx** (strong recommendation)

This schema is a textbook case for **explicit SQL with type-safe bindings**.

### Why sqlc fits perfectly here

| Schema reality               | Why sqlc wins                              |
| ---------------------------- | ------------------------------------------ |
| DB is source of truth        | sqlc generates from SQL, not structs       |
| Join-heavy auth              | You *want* to write these joins explicitly |
| Stable identifiers           | No ORM identity map issues                 |
| Protocol correctness matters | SQL is precise and auditable               |
| Mostly reads                 | Zero ORM overhead                          |
| GA4GH compliance             | Easier to prove correctness                |

You will inevitably write queries like:

```sql
SELECT am.*
FROM access_method am
JOIN file_access_object fao ON fao.id = am.file_access_object_id
JOIN drs_object_visa dov ON dov.drs_object_id = fao.drs_object_id
JOIN passport_visa v ON v.id = dov.visa_id
WHERE v.issuer = $1;
```

That is **exactly** what sqlc is built for.

---

## What about ORMs?

### ❌ Ent (not a good fit *here*)

Ent is excellent — **but only when Go owns the schema**.

Problems for this project:

* You’d duplicate the SQL schema in Go
* Risk schema drift
* Authorization joins become awkward
* JSON / protocol semantics aren’t buying you anything

### ⚠️ sqlboiler (acceptable, but second best)

If you *must* have ORM-style models:

* sqlboiler can generate from this schema
* But you’ll still write lots of custom SQL
* You’ll end up halfway between ORM and sqlc anyway

### ❌ GORM (actively discouraged)

* Reflection-heavy
* Implicit behavior
* Poor fit for auth-driven joins
* Harder to reason about correctness in a standards-based service

---

## Concrete recommendation

**Use this stack:**

```text
Postgres
pgx
sqlc
handwritten SQL
```

**Do NOT:**

* Try to model `passport_visa` or `drs_object_visa` as “objects”
* Hide authorization logic in application code
* Let an ORM invent query behavior for you

---

## Suggested project layout

```text
database/
  postgresql/
    create-tables.sql

queries/
  drs_objects.sql
  access_methods.sql
  bundles.sql
  authorization.sql

internal/db/
  sqlc/
    queries.sql.go
    models.go
```

Each DRS endpoint maps cleanly to **1–2 SQL queries**.
Handlers stay thin. Semantics stay correct.

---

## Bottom line

> For *this exact schema* and a GA4GH DRS implementation:
>
> **sqlc + pgx is the best possible choice.**



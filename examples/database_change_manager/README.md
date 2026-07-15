# Database change manager

Plans, applies, verifies, and reports a SQLite schema change. The database artifact
and every decision are inspectable.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom plan_schema_change
mwf inspect apply_schema_change job 1
mwf inspect verify_database failed
```

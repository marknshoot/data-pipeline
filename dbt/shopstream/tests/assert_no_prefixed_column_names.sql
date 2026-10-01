-- Guard against a subtle ClickHouse behaviour: when a SELECT joins two relations
-- that share a column name, an un-aliased `alias.column` is created with that
-- qualified name ("o.user_id") instead of "user_id". Every mart column must be
-- explicitly aliased. This test fails if any name still contains a dot.
--
-- Fails (returns rows) when a prefixed column exists.

select
    database,
    table,
    name as column_name
from system.columns
where database = 'marts'
  and position(name, '.') > 0

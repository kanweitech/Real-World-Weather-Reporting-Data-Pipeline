{% macro delete_insert() %}
    do $$
    begin
        if exists (
            select from information_schema.tables
            where table_schema = 'dev' and table_name = 'weather_del_ins'
        ) then
            execute 'truncate table dev.weather_del_ins';
        end if;
    end
    $$;
{% endmacro %}
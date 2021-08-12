CREATE TABLE dq_master(
    master_id int,
    master_status int
);

CREATE TABLE dq_rule(
    rule_id int,
    master_id int,
    param_sql_file string,
    summary_sql_file string,
    granular_sql_file string,
    rule_status int
);

CREATE TABLE dq_granular_exception(
    rule_id int,

    except_dt timestamp
);

CREATE TABLE dq_summary_exception(
    rule_id int,
    
    except_dt timestamp
);
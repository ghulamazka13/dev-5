class MetadataQuery:
    def __init__(self):
        self.database_connections = """
        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(d)), '[]'::json)
        FROM (
            SELECT id, db_name, db_type, db_host, db_port, username, db_conn_name, gsm_path
            FROM control.database_connections
            ORDER BY id
        ) d
        """

        self.dag_configs = """
        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(d)), '[]'::json)
        FROM (
            SELECT id, dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks
            FROM control.dag_configs
            WHERE enabled IS TRUE
            ORDER BY dag_name
        ) d
        """

        self.datasource_to_dwh = """
        WITH databases AS (
            SELECT dc.id, ROW_TO_JSON(dc) AS database_conn
            FROM (
                SELECT id, db_name, db_type, db_host, db_port, username, db_conn_name, gsm_path
                FROM control.database_connections
            ) dc
        ),
        pipelines_base AS (
            SELECT
                p.id AS pipeline_db_id,
                p.pipeline_id,
                p.dag_id,
                p.enabled,
                p.description,
                p.datasource_table,
                p.datawarehouse_table,
                p.source_db_id,
                p.target_db_id,
                COALESCE(p.source_table_name, p.datasource_table) AS source_table_name,
                p.source_sql_query,
                p.datasource_timestamp_column,
                COALESCE(p.target_schema, split_part(p.datawarehouse_table, '.', 1)) AS target_schema,
                COALESCE(p.target_table_name, split_part(p.datawarehouse_table, '.', 2)) AS target_table_name,
                p.target_table_schema,
                p.unique_key,
                p.merge_window_minutes,
                p.expected_columns,
                p.sql_merge_path,
                p.freshness_threshold_minutes,
                p.sla_minutes
            FROM control.datasource_to_dwh_pipelines p
            WHERE p.enabled IS TRUE
        ),
        pipelines AS (
            SELECT
                pb.pipeline_id,
                pb.pipeline_db_id,
                pb.dag_id,
                pb.enabled,
                pb.description,
                pb.source_table_name,
                pb.source_sql_query,
                pb.datasource_timestamp_column,
                pb.target_schema,
                pb.target_table_name,
                pb.target_table_schema,
                pb.unique_key,
                pb.merge_window_minutes,
                pb.expected_columns,
                pb.sql_merge_path,
                pb.freshness_threshold_minutes,
                pb.sla_minutes,
                COALESCE(
                    (
                        SELECT JSON_AGG(ROW_TO_JSON(s) ORDER BY s.step_order, s.id)
                        FROM (
                            SELECT id, step_name, step_order, sql_text
                            FROM control.datasource_to_dwh_sql_steps
                            WHERE pipeline_db_id = pb.pipeline_db_id
                              AND enabled IS TRUE
                            ORDER BY step_order, id
                        ) s
                    ),
                    '[]'::json
                ) AS sql_steps,
                pb.source_db_id,
                pb.target_db_id,
                src.database_conn AS source_database_conn,
                tgt.database_conn AS target_database_conn,
                pb.source_table_name AS datasource_table,
                COALESCE(pb.datawarehouse_table, pb.target_schema || '.' || pb.target_table_name) AS datawarehouse_table
            FROM pipelines_base pb
            LEFT JOIN databases src ON pb.source_db_id = src.id
            LEFT JOIN databases tgt ON pb.target_db_id = tgt.id
        )
        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(f)), '[]'::json) FROM (
            SELECT
                dc.id AS dag_id,
                dc.dag_name,
                dc.enabled,
                dc.schedule_cron,
                dc.timezone,
                dc.owner,
                dc.tags,
                dc.max_active_tasks,
                JSON_AGG(p ORDER BY p.pipeline_id) AS pipelines
            FROM pipelines p
            INNER JOIN control.dag_configs dc ON p.dag_id = dc.id
            WHERE dc.enabled IS TRUE
            GROUP BY 1,2,3,4,5,6,7,8
            ORDER BY 2
        ) f
        """

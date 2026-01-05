class MetadataQuery:
    def __init__(self):
        self.slave = """
        WITH dataprocs AS (
            SELECT 	dc.id, 
                    ROW_TO_JSON(dc) AS dataproc_config
            FROM 
                (
                  SELECT  id,
                          zone_uri,
                          project_id,
                          network_uri,
                          temp_bucket,
                          cluster_name,
                          config_bucket,
                          master_config,
                          worker_config,
                          subnetwork_uri,
                          service_account,
                          region
                  FROM dataproc_configs
                ) dc
        ), databases AS (
            SELECT 	dc.id, 
                    ROW_TO_JSON(dc) AS database_conn
            FROM 
                (
                  SELECT  id,
                          db_name,
                          db_type,
                          db_port,
                          gsm_path,
                          username,
                          db_conn_name
                  FROM database_connections
                ) dc
        ), pipelines AS (
            SELECT 	stbs.db_id,
                    stbs.dag_id,
                    stbs.dataproc_id,
                    (
                        SELECT ROW_TO_JSON(d)
                        FROM (
                            SELECT	stb.source_table_name,
                                    stb.source_sql_query,
                                    stb.target_project_id,
                                    stb.target_dataset,
                                    stb.target_table_name,
                                    stb.target_table_schema,
                                    stb.write_method,
                                    stb.cluster_fields,
                                    stb.pyfile_bucket,
                                    stb.gcs_bucket,
                                    stb.processor,
                                    stb.time_partitioning,
                                    stb.is_external_table,
                                    stb.initial_load,
                                    stb.queue,
                                    stb.export_format
                            FROM slave_to_bq stb
                            WHERE stb.id = stbs.id AND stb.enabled IS TRUE
                        ) d
                    ) AS slave_task
            FROM slave_to_bq stbs
            WHERE stbs.enabled IS TRUE
        )
        SELECT JSON_AGG(ROW_TO_JSON(f)) FROM (
          SELECT dc.dag_name, dc.owner, dc.schedule, dc.tags, dc.start_date, dc.max_active_tasks, JSON_AGG(d) AS slave_pipelines
          FROM (
                    SELECT 	pipelines.dag_id,
                            databases.database_conn,
                            dataprocs.dataproc_config,
                            pipelines.slave_task
                    FROM pipelines
                    INNER JOIN databases ON pipelines.db_id = databases.id
                    INNER JOIN dataprocs ON pipelines.dataproc_id = dataprocs.id
          ) d INNER JOIN dag_configs dc ON d.dag_id = dc.id
          GROUP BY 1, 2, 3, 4, 5, 6
        ) f
        """

        self.db2db = """
        WITH dataprocs AS (
        SELECT
            dc.id,
            ROW_TO_JSON(dc) AS dataproc_config
        FROM (
            SELECT
            id,
            zone_uri,
            project_id,
            network_uri,
            temp_bucket,
            cluster_name,
            config_bucket,
            master_config,
            worker_config,
            subnetwork_uri,
            service_account,
            region
            FROM public.dataproc_configs
        ) dc
        ),
        databases AS (
        SELECT
            dc.id,
            ROW_TO_JSON(dc) AS database_conn
        FROM (
            SELECT
            id,
            db_name,
            db_type,
            db_port,
            gsm_path,
            username,
            db_conn_name
            FROM public.database_connections
        ) dc
        ),
        pipelines AS (
        SELECT
            d2d.db_id,            -- source
            d2d.dst_db_id,        -- destination
            d2d.dag_id,
            d2d.dataproc_id,
            (
            SELECT ROW_TO_JSON(d)
            FROM (
                SELECT
                d2d.source_table_name,
                d2d.source_sql_query,
                d2d.target_schema,
                d2d.target_table_name,
                d2d.target_table_schema,
                d2d.write_method,
                d2d.pk_keys_ref,
                d2d.queue,
                d2d.disable_fk_during_load
            ) d
            ) AS db2db_task
        FROM public.db_to_db d2d
        WHERE d2d.enabled IS TRUE
        )
        SELECT JSON_AGG(ROW_TO_JSON(f)) FROM (
        SELECT
            dc.dag_name,
            dc.owner,
            dc.schedule,
            dc.tags,
            dc.start_date,
            dc.max_active_tasks,
            JSON_AGG(d) AS db2db_pipelines
        FROM (
            SELECT
            p.dag_id,
            src.database_conn AS src_database_conn,
            dst.database_conn AS dst_database_conn,
            dp.dataproc_config,
            p.db2db_task
            FROM pipelines p
            INNER JOIN databases src ON p.db_id     = src.id
            INNER JOIN databases dst ON p.dst_db_id = dst.id
            LEFT  JOIN dataprocs dp  ON p.dataproc_id = dp.id
        ) d
        INNER JOIN public.dag_configs dc ON d.dag_id = dc.id
        GROUP BY 1,2,3,4,5,6
        ) f
        """

        self.bq = """
        WITH
            checker AS (
                        SELECT 
                            f.bq_to_bq_id, 
                            JSONB_AGG(f) checker_tasks
                        FROM ( 
                            SELECT 
                                b.bq_to_bq_id, 
                                ROW_TO_JSON(b) checker
                            FROM ( 
                                SELECT 
                                    bq_to_bq_id,
                                    task_name,
                                    external_dag_id,
                                    external_task_group_id,
                                    external_task_id,
                                    delta_days,
                                    delta_hours,
                                    delta_minutes,
                                    poke_interval,
                                    "mode",
                                    timeout
                                FROM 
                                    public.bq_checker
                                WHERE
                                    enabled IS TRUE
                            ) b 
                        ) f
                        GROUP BY f.bq_to_bq_id
                    ),
            pipelines AS (
                        SELECT 	bbs.dag_id,
                                (
                                    SELECT ROW_TO_JSON(d)
                                    FROM (
                                        SELECT	bb.query,
                                                bb.target_project_id,
                                                bb.target_dataset,
                                                bb.target_table_name,
                                                bb.wait_for,
                                                bb.write_method,
                                                bb.cluster_fields,
                                                bb.time_partitioning,
                                                bb.queue,
                                                checker.checker_tasks
                                        FROM bq_to_bq bb
                                        left join checker on bb.id = checker.bq_to_bq_id
                                        WHERE bb.id = bbs.id AND bb.enabled IS TRUE
                                    ) d
                                ) AS bq_task
                        FROM bq_to_bq bbs
                        WHERE bbs.enabled IS TRUE
                    )
                    SELECT JSON_AGG(ROW_TO_JSON(f)) FROM (
                    SELECT dc.dag_name, dc.owner, dc.schedule, dc.tags, dc.start_date, dc.max_active_tasks, JSON_AGG(d) AS bq_pipelines
                    FROM (
                                SELECT 	pipelines.dag_id,
                                        pipelines.bq_task
                                FROM pipelines
                    ) d INNER JOIN dag_configs dc ON d.dag_id = dc.id
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ) f
        """

        self.wait_for_check = """
        WITH wt AS (
          SELECT
            CONCAT(bq.target_project_id,'.',bq.target_dataset,'.',bq.target_table_name) AS upstream
          FROM public.bq_to_bq bq
          WHERE enabled IS TRUE
          
          UNION ALL
          
          SELECT
            CONCAT(stbq.target_project_id,'.',stbq.target_dataset,'.',stbq.target_table_name) AS upstream
          FROM public.slave_to_bq stbq
          WHERE enabled IS TRUE
        )
        SELECT
          JSON_AGG(wt.upstream)
        FROM wt
        """

        self.dq = """
            with dq as (
                select 
                    source_config_id,
                    target_config_id,
                    rule_config_id,
                    dag_id,
                    dq_name,
                    source_query,
                    target_query,
                    column_name,
                    toleration_score,
                    alert
                from dq_configs
                where enabled is true
            ), src as (
                select
                    id,
                    db_id,
                    source_table_name
                from source_configs
            ), trgt as (
                select
                    id,
                    project_id,
                    dataset_id,
                    table_name
                from target_configs
            ), db as (
                select
                    id,
                    db_name,
                    db_type,
                    db_host,
                    db_port,
                    username,
                    gsm_path,
                    db_conn_name
                from database_connections
            ), dag as (
                select
                    id,
                    dag_name,
                    schedule,
                    tags,
                    owner,
                    start_date
                from dag_configs
            ), rules as (
                select
                    id, 
                    rule_name,
                    rule_template
                from rule_configs
            )
            
            select json_agg(all_configs) from (
            select
                dag.dag_name,
                dag.schedule,
                dag.tags,
                dag.owner,
                dag.start_date,
                json_agg(results) as dq_pipelines
            from (
                    select
                        db.db_name,
                        db.db_type,
                        db.db_host,
                        db.db_port,
                        db.username,
                        db.db_conn_name,
                        dag.id as dag_id,
                        db.id as db_id,
                        dq.dq_name,
                        concat(trgt.project_id, '.', trgt.dataset_id, '.', trgt.table_name) as bq_table,
                        src.source_table_name as source_table,
                        dq.source_query,
                        dq.target_query,
                        dq.column_name,
                        dq.toleration_score,
                        rules.rule_name,
                        rules.rule_template,
                        dq.alert
                    from dq
                    left join src on dq.source_config_id = src.id
                    left join db on src.db_id = db.id
                    left join dag on dq.dag_id = dag.id
                    left join trgt on dq.target_config_id = trgt.id
                    left join rules on dq.rule_config_id = rules.id
            ) results
            left join dag on results.dag_id = dag.id
            group by 1,2,3,4,5) all_configs
        """

        self.dag_registered = """
            select 
                json_agg(dag_name) 
            from 
                public.dag_configs
        """

        self.gsheet = """
        WITH
            pipelines AS (
                        SELECT 	gtbs.dag_id,
                                (
                                    SELECT ROW_TO_JSON(d)
                                    FROM (
                                        SELECT	gtb.dag_id,
                                                	gtb.spreadsheet_id,
                                                	gtb.worksheet_name,
                                                	gtb.project_id,
                                                	gtb.dataset_name,
                                                	gtb.target_table_name,
                                                	gtb.target_schema,
                                                	gtb.write_disposition,
                                                	gtb.time_partitioning,
                                                	gtb.schema_update_option
                                        FROM public.gsheet_to_bq gtb
                                        WHERE gtb.id = gtbs.id AND gtb.enabled IS TRUE
                                    ) d
                                ) AS gsheet_task
                        FROM public.gsheet_to_bq gtbs
                        WHERE gtbs.enabled IS TRUE
                    )
                    SELECT JSON_AGG(ROW_TO_JSON(f)) FROM (
                    SELECT dc.dag_name, dc.owner, dc.schedule, dc.tags, dc.start_date, dc.max_active_tasks, JSON_AGG(d) AS gsheet_pipelines
                    FROM (
                                SELECT 	pipelines.dag_id,
                                        pipelines.gsheet_task
                                FROM pipelines
                    ) d INNER JOIN dag_configs dc ON d.dag_id = dc.id
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ) f
        """

        self.inbox = """
        WITH dag AS (
            SELECT
                id,
                dag_name,
                schedule,
                tags,
                owner,
                start_date,
                max_active_tasks
            FROM public.dag_configs
        )
        SELECT json_agg(all_configs) FROM (
            SELECT
                dag.dag_name,
                dag.schedule,
                dag.tags,
                dag.owner,
                dag.start_date,
                dag.max_active_tasks,
                row_to_json(results)::jsonb as configs 
            FROM (
                SELECT 
                    f.dag_id,
                    json_agg(row_to_json(f)::jsonb - 'dag_id') AS inbox_pipelines
                FROM (
                    SELECT 
                        bi.id, 
                        bi.dag_id, 
                        bi.application_id, 
                        bi.service, 
                        bi.view_name, 
                        bi.requirements, 
                        bi.queue,
                        ic.url,
                        ic.image_url,
                        ic.api_key,
                        ic.broadcast_name,
                        ic.category,
                        ic.event_type,
                        ic."event",
                        ic.reference_id,
                        ic.link,
                        ic.title,
                        ic.short_description,
                        ic.description,
                        ic.status,
                        ic.recipient
                    FROM public.bq_to_inbox bi
                    INNER JOIN public.inbox_configs ic ON bi.inbox_config_id = ic.id 
                    WHERE 
                        enabled IS TRUE
                ) f
                GROUP BY 1
            ) results
            INNER JOIN dag ON results.dag_id = dag.id
            -- GROUP BY 1,2,3,4,5
        ) all_configs
        """

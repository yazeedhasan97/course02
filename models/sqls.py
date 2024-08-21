FEED = "feed"
TBL_DT = "tbl_dt"

FILE_NAME = "file_name"
COUNT = "counts"
PARTITION = "partition"

######################################################################################################################
######################################################################################################################
######################################################################################################################
NEW_FILE_STATS_EXISTENCE_QUERY = f"""
    SELECT
        UPPER(REPLACE(fsys_msgtype, 'com.mtn.messages.', '')) AS {FEED},
        tbl_dt AS {TBL_DT},
        from_unixtime(fsys_landing_ts / 1000) as {COUNT} """ + """
    FROM
        feeds.file_stats
    WHERE
        UPPER(fsys_msgtype) LIKE '%{feed}'
        and tbl_dt BETWEEN {past} and {today}
"""

# -- and tbl_dt BETWEEN CAST(date_format(CURRENT_DATE - INTERVAL '{search_within_past_days}' DAY, '%Y%m%d') AS INTEGER )
# --                AND CAST(date_format(CURRENT_DATE, '%Y%m%d') AS INTEGER)

OPS_LAST_AVAILABLE_DATE_QUERY = f"""
    select 
        max(tbl_dt) as {TBL_DT}  """ + """
    from 
        {file_ops_schema}.FILES_FILESOPPS_SUMMARY
    where 
        upper(path) like '%/{feed}/%'
        and tbl_dt BETWEEN {past} and {today}
"""

# TODO: needs more consideration for the partitioned_by column when it is multiple
# TODO: ask CHatGPT if this is the best scenario to scan partitions
HIVE_LAST_AVAILABLE_DATE_QUERY = """
    select 
        max({partitioned_by}) """ + f""" as {TBL_DT} """ + """
    from 
        {schema}.{feed}
    where 
        {partitioned_by} BETWEEN {past} and {today}
"""

######################################################################################################################

MSCK_QUERY = """ msck repair table {schema}.{feed} """

# TODO: raise request to make sure it is available in all OPCO
# TODO: new script to automatically fill the database inputs
DISTRIBUTED_PARTITIONS_DATES_QUERY = """
    SELECT
        DISTINCT fsys_event_date AS event_date
    FROM
        {file_transdate_schema}.filetransdatemsgbydate 
    WHERE
        file_date = {run_date}
        AND upper(fsys_msgtype) LIKE '%{feed}'
"""

######################################################################################################################

# TODO: what to do if 1 or more files from another feed has the same name in it -- example PM_PARTNER
#   Currently I added '%/{feed}/%' for the path, but I am not sure it is feasible
# TODO: fix the name of the schema to read from outer source # feeds
#   the path regex problem
# PM_PARTNER_INDETEFIR
# PM_PARTNER_new/
FILE_OPS_FILES_AND_COUNTS_SQL_EXTENDED = f""" 
    select
        DISTINCT NAME AS {FILE_NAME},
        LINES as {COUNT} """ + """
    from
        {file_ops_schema}.FILES_FILESOPPS_SUMMARY
    where
        tbl_dt in ({distributed_partitions_dates})
        and LINES > 0
        and upper(path) like '%/{feed}/%' 
        {extend}
    group by 
        NAME,
        LINES
"""

# TODO: check the file names with the @ behavior in the name for FDI feed
# TODO: fix the COALESCE default to 0 for failures -- V2

HIVE_TABLE_FILES_AND_COUNTS_SQL_EXTENDED = f"""
    select
        -- SPLIT(REVERSE(ELEMENT_AT(SPLIT(REVERSE(file_name), '/'), 1)), '.0')[1]  AS {FILE_NAME},
        base_file_name AS {FILE_NAME},
        COUNT(*) AS {COUNT} """ + """
    FROM
        {schema}.{feed}
    WHERE
        {partitioned_by} IN ({distributed_partitions_dates}) 
        {extend}
    GROUP BY
        -- SPLIT(REVERSE(ELEMENT_AT(SPLIT(REVERSE(file_name), '/'), 1)), '.0')[1]
        base_file_name
"""

REJECTED_FILES_AND_COUNTS_SQL_EXTENDED = f"""
    select
        fsys_filename AS {FILE_NAME},
        fsys_fileoffset  AS {COUNT} """ + """
    from
        feeds.rejecteddata
    where
        file_date = {run_date}
        and msgtype = lower('com.mtn.messages.{feed}')
        {extend}
    group by
        fsys_filename,
        fsys_fileoffset
"""

DUPLICATED_FILES_AND_COUNTS_SQL_EXTENDED = f"""
    select
        reverse(element_at(split(reverse(file_name), '/'), 1)) AS {FILE_NAME},
        file_offset AS {COUNT} """ + """
    from
        feeds.dupdata
    where
        tbl_dt in ({distributed_partitions_dates})
        and upper(feedtype) = upper('{feed}')
        {extend}
    group by
        reverse(element_at(split(reverse(file_name), '/'), 1)),
        file_offset
"""
######################################################################################################################

# TODO: fix to handle multiple partitions
TREND_QUERY = """
    select
        {partitioned_by} """ + f"AS {TBL_DT}, count(*) AS {COUNT} " + """
    from
        {schema}.{feed}
    where
        {partitioned_by} in ({dates})
    group by
        {partitioned_by}
"""
########################################################### Second Partitioning For Dedup
# FYI: up to this date 2024/05/08 LD didn't implement more than 2 partitions for any feed
# TODO: need to adapt for more than 2 partitions -- V2 - we already have the place holder, fix the query only
# TODO: not all feeds second partition works for dedup
SECOND_PARTITION_UNIQUE_VALUES_QUERY = """
    select 
        distinct {second_partition} as """ + f"{PARTITION}" + """
    from 
        {schema}.{feed}
    where 
        {first_partition} = {run_date}
"""

########################################################### TimeLiness -- V2
# TODO: need to be tested and activated - V2
TIMELINES_DIFFERENCES_QUERY = f"""
    select avg({COUNT}) from (
            SELECT
                replace(fsys_msgtype, 'com.mtn.messages.', '') as {FILE_NAME},
                avg((starttimets - fsys_landing_ts)/ 1000 / 60.0) as {COUNT},
                tbl_dt as {TBL_DT}
            FROM
                    feeds.file_stats """ + """
            WHERE
                    tbl_dt in {distributed_partitions_dates}
                    and upper(replace(fsys_msgtype, 'com.mtn.messages.', '')) = upper('{feed}') 
            GROUP BY
                    fsys_msgtype,
                    tbl_dt
    ) """ + f"""
    group by {FILE_NAME}, {TBL_DT}
    having count(*) = 1;
"""

######################################################################################################################
######################################################################################################################
######################################################################################################################

from models.models import SCHEMA

RUNNING_STATUS_PROCEDURE_CREATOR = f"""
CREATE OR REPLACE PROCEDURE {SCHEMA}.update_status_if_running_too_long(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_updated_at_column_name TEXT,
    p_status_columns TEXT[],
    p_minutes_threshold INTEGER,
    p_reason_column TEXT DEFAULT 'reason',
    p_target_status TEXT DEFAULT 'FAILED'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_status_column TEXT;
BEGIN
    -- Validate threshold
    IF p_minutes_threshold <= 0 THEN
        RAISE EXCEPTION 'Minutes threshold must be a positive integer.';
    END IF;

    -- Ensure the ENUM type is recognized by setting the search path
    SET search_path TO data_quality;

    -- Begin transaction
    BEGIN
        -- Iterate over each status column provided in the array
        FOREACH v_status_column IN ARRAY p_status_columns LOOP
            -- Construct the dynamic SQL to update the status and reason
            v_sql := format('
                UPDATE %I.%I 
                SET %I = $1, %I = format(''The run is stuck after running for %s minutes without results.'', EXTRACT(EPOCH FROM (now() - %I))/60)
                WHERE %I = ''RUNNING'' 
                -- /60 here converts seconds to minutes
                AND EXTRACT(EPOCH FROM (now() - %I))/60 > $2
                AND %I >= now() - interval ''3 days''',
                p_schema_name, p_table_name, v_status_column, p_reason_column, p_updated_at_column_name, v_status_column, 
                p_updated_at_column_name, p_updated_at_column_name);

            -- Execute the dynamic SQL with parameters
            EXECUTE v_sql USING p_target_status, p_minutes_threshold;

            -- Log to the console
            RAISE NOTICE 'Procedure executed on %I.%I for column %I. Rows updated: %', p_schema_name, p_table_name, v_status_column, ROW_COUNT();
        END LOOP;

        -- Commit the transaction
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            -- Rollback transaction in case of error
            ROLLBACK;
            -- Handle errors
            RAISE NOTICE 'An error occurred: %', SQLERRM;
            -- Optionally, you could RAISE the exception again or perform some rollback actions here
            -- RAISE;
    END;
END;
$$;

"""

CALLING_STATUS_PROCEDURE_SCHEDULES_CREATOR = f"""
CALL {SCHEMA}.update_status_if_running_too_long(
    :schema_name,
    :table_name,
    :updated_at_column_name,
    :status_columns,
    :minutes_threshold,
    :reason_column,
    :target_status
)
"""

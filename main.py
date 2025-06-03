import psycopg2
import psycopg2.extras
import os
import traceback


# --- PostgreSQL Database Connection Configuration ---
DB_NAME_PG_DEFAULT = "postgres"
DB_USER_PG_DEFAULT = "postgres"
DB_PASS_PG_DEFAULT = "12052004"
DB_HOST_PG_DEFAULT = "localhost"
DB_PORT_PG_DEFAULT = 5432

# --- Constants for Partitioning ---
MIN_RATING_CONST = 0.0
MAX_RATING_CONST = 5.0

DATABASE_NAME = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'


def _get_db_connection_pg_internal(dbname=DB_NAME_PG_DEFAULT, user=DB_USER_PG_DEFAULT,
                                   password=DB_PASS_PG_DEFAULT, host=DB_HOST_PG_DEFAULT, port=DB_PORT_PG_DEFAULT):
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        return conn
    except psycopg2.Error as e:
        print(f"🚫 Internal PostgreSQL connection error: {e}")
        return None


def _execute_query_pg_with_provided_conn(conn, query, params=None, fetch=False):
    if not conn or conn.closed:
        raise psycopg2.InterfaceError("Connection invalid or closed in _execute_query_pg")
    result = None
    try:
        with conn.cursor() as cur:
            cur.execute(query, params if params else None)
            if fetch == 'one':
                result = cur.fetchone()
            elif fetch == 'all':
                result = cur.fetchall()
            if not conn.autocommit:
                conn.commit()
    except psycopg2.Error as e:
        print(f"SQLException (PostgreSQL) in _execute_query_pg: {e}")
        print(f"Failed query: {query}")
        if params:
            print(f"Parameters: {params}")
        if not conn.autocommit and conn and not conn.closed:
            try:
                if conn.status == psycopg2.extensions.STATUS_IN_ERROR:
                    conn.rollback()
            except psycopg2.Error as rb_err:
                print(f"Error during rollback in _execute_query_pg: {rb_err}")
        raise
    return result if fetch else True


def loadratings(ratingstablename, file_path, openconnection):
    actual_table_name = DATABASE_NAME
    print(f"⏳ Starting data load from: {file_path} into table \"{actual_table_name}\" (PostgreSQL).")

    conn_load = openconnection
    if not conn_load or conn_load.closed:
        raise Exception("loadratings: Invalid PostgreSQL connection.")

    # Step 0: Ensure the main table exists with lowercase columns
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {actual_table_name} (
        userid INT,
        movieid INT,
        rating REAL
    );"""
    try:
        print(f"INFO: Ensuring table {actual_table_name} exists with lowercase columns...")
        _execute_query_pg_with_provided_conn(conn_load, create_table_sql)
        print(f"✅ Table {actual_table_name} checked/created.")
    except Exception as e_create:
        print(f"🚫 Error creating table {actual_table_name}: {e_create}")
        raise

    print(f"INFO: Deleting old data from table {actual_table_name} (PostgreSQL)...")
    _execute_query_pg_with_provided_conn(conn_load, f'DELETE FROM {actual_table_name};')
    print(f"✅ Old data deleted from table {actual_table_name}.")

    insert_sql_template = f'INSERT INTO {actual_table_name} (userid, movieid, rating) VALUES %s;'

    lines_per_chunk = 500000
    records_in_current_chunk = []
    total_records_inserted = 0
    total_lines_read = 0
    warnings_count = 0
    max_warnings_to_print = 5

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            with conn_load.cursor() as cur_load:
                for line_count, line in enumerate(f):
                    total_lines_read = line_count + 1
                    parts = line.strip().split('::')
                    if len(parts) == 4:
                        try:
                            userid = int(parts[0])
                            movieid = int(parts[1])
                            rating_val = float(parts[2])
                            records_in_current_chunk.append((userid, movieid, rating_val))
                        except ValueError:
                            warnings_count += 1
                            if warnings_count <= max_warnings_to_print:
                                print(f"⚠️ Skipping invalid line (L{total_lines_read}): {line.strip()}")
                            continue
                    else:
                        warnings_count += 1
                        if warnings_count <= max_warnings_to_print:
                            print(f"⚠️ Skipping malformed line (L{total_lines_read}): {line.strip()}")

                    if len(records_in_current_chunk) >= lines_per_chunk:
                        if records_in_current_chunk:
                            psycopg2.extras.execute_values(cur_load, insert_sql_template, records_in_current_chunk,
                                                           page_size=lines_per_chunk)
                            total_records_inserted += len(records_in_current_chunk)
                            records_in_current_chunk = []

                if records_in_current_chunk:
                    psycopg2.extras.execute_values(cur_load, insert_sql_template, records_in_current_chunk,
                                                   page_size=len(records_in_current_chunk))
                    total_records_inserted += len(records_in_current_chunk)

                if not conn_load.autocommit:
                    conn_load.commit()

        if warnings_count > max_warnings_to_print:
            print(f"... and {warnings_count - max_warnings_to_print} more warnings.")
        print(f"✅ Data load complete! {total_records_inserted} records added to table '{actual_table_name}'.")
        return total_records_inserted
    except Exception as e:
        print(f"🚫 Error in loadratings: {e}")
        if conn_load and not conn_load.closed and not conn_load.autocommit:
            try:
                if conn_load.status == psycopg2.extensions.STATUS_IN_ERROR:
                    conn_load.rollback()
            except psycopg2.Error:
                pass
        raise


def rangepartition(ratingstablename, n, openconnection):
    actual_base_table_name = ratingstablename.lower()
    if n <= 0:
        raise ValueError("N must be > 0 for rangepartition")

    print(f"⏳ Partitioning table '{actual_base_table_name}' into {n} parts by 'Rating' (PostgreSQL)...")

    conn_part = openconnection
    if not conn_part or conn_part.closed:
        raise Exception("rangepartition: Invalid PostgreSQL connection.")

    range_step = (MAX_RATING_CONST - MIN_RATING_CONST) / n
    if range_step == 0:
        raise ValueError("range_step is zero, check MIN_RATING_CONST, MAX_RATING_CONST, or n.")

    previous_upper_bound = MIN_RATING_CONST

    for i in range(n):
        part_table_name = f"{RANGE_TABLE_PREFIX}{i}"

        current_upper = MIN_RATING_CONST + (i + 1) * range_step
        if i == n - 1:
            current_upper = MAX_RATING_CONST

        _execute_query_pg_with_provided_conn(conn_part, f'DROP TABLE IF EXISTS {part_table_name} CASCADE;')
        create_sql = f"""CREATE TABLE {part_table_name} (userid INT, movieid INT, rating REAL);"""
        _execute_query_pg_with_provided_conn(conn_part, create_sql)

        condition = ""
        if i == 0:
            condition = f"rating >= {MIN_RATING_CONST} AND rating <= {current_upper}"
        else:
            condition = f"rating > {previous_upper_bound} AND rating <= {current_upper}"

        insert_sql = f"""INSERT INTO {part_table_name} (userid, movieid, rating)
                         SELECT userid, movieid, rating FROM {actual_base_table_name} WHERE {condition};"""
        _execute_query_pg_with_provided_conn(conn_part, insert_sql)
        previous_upper_bound = current_upper
        print(f"  ✅ Created partition {part_table_name}")
    print(f"🎉 Range partitioning successful! Created {n} partitions.")


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    actual_base_table_name = ratingstablename.lower()
    MovieID = itemid
    RatingVal = float(rating)
    UserID = userid

    print(f"⏳ Range Insert (PostgreSQL): UserID={UserID}, MovieID={MovieID}, Rating={RatingVal}")

    conn_insert_range = openconnection
    if not conn_insert_range or conn_insert_range.closed:
        raise Exception("rangeinsert: Invalid PostgreSQL connection.")

    insert_original_sql = f'INSERT INTO {actual_base_table_name} (userid, movieid, rating) VALUES (%s, %s, %s);'
    _execute_query_pg_with_provided_conn(conn_insert_range, insert_original_sql, (UserID, MovieID, RatingVal))
    print(f"  👍 Inserted into original table {actual_base_table_name}.")

    N = get_number_of_partitions_pg_with_conn(conn_insert_range, RANGE_TABLE_PREFIX)

    if N == 0:
        print("⚠️ No range partitions found.");
        return

    if N == 1:
        target_part_table_name = f"{RANGE_TABLE_PREFIX}0"
        if not (MIN_RATING_CONST <= RatingVal <= MAX_RATING_CONST):
            print(f"⚠️ Warning: Rating {RatingVal} is not within any partition range (N=1).")
            return
    else:
        range_step = (MAX_RATING_CONST - MIN_RATING_CONST) / N
        if range_step == 0:
            raise ValueError("range_step is zero.")

        target_part_table_name = None
        previous_upper_bound = MIN_RATING_CONST
        for i in range(N):
            current_part_name_candidate = f"{RANGE_TABLE_PREFIX}{i}"
            current_upper_check = MIN_RATING_CONST + (i + 1) * range_step
            if i == N - 1:
                current_upper_check = MAX_RATING_CONST

            if i == 0:
                if RatingVal >= MIN_RATING_CONST and RatingVal <= current_upper_check:
                    target_part_table_name = current_part_name_candidate
                    break
            else:
                if RatingVal > previous_upper_bound and RatingVal <= current_upper_check:
                    target_part_table_name = current_part_name_candidate
                    break
            previous_upper_bound = current_upper_check

    if target_part_table_name:
        print(f"  🎯 Data will be inserted into partition {target_part_table_name} for Rating {RatingVal}.")
        insert_partition_sql = f'INSERT INTO {target_part_table_name} (userid, movieid, rating) VALUES (%s, %s, %s);'
        _execute_query_pg_with_provided_conn(conn_insert_range, insert_partition_sql, (UserID, MovieID, RatingVal))
        print(f"  ✅ Successfully inserted into partition {target_part_table_name}.")
    else:
        print(f"⚠️ Warning: Rating {RatingVal} does not fall into any partition range.")


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    actual_base_table_name = ratingstablename.lower()
    N = numberofpartitions
    if N <= 0:
        raise ValueError("N must be > 0")
    print(f"⏳ Partitioning table '{actual_base_table_name}' into {N} parts by Round Robin (PostgreSQL)...")

    conn_rr_part = openconnection
    if not conn_rr_part or conn_rr_part.closed:
        raise Exception("roundrobinpartition: Invalid PostgreSQL connection.")

    for i in range(N):
        part_table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        _execute_query_pg_with_provided_conn(conn_rr_part, f'DROP TABLE IF EXISTS {part_table_name} CASCADE;')
        create_sql = f"""CREATE TABLE {part_table_name} (userid INT, movieid INT, rating REAL);"""
        _execute_query_pg_with_provided_conn(conn_rr_part, create_sql)
        print(f"  Created partition table {part_table_name}.")

    all_data = []
    try:
        with conn_rr_part.cursor() as cur_select:
            cur_select.execute(f'SELECT userid, movieid, rating FROM {actual_base_table_name};')
            all_data = cur_select.fetchall()
    except psycopg2.Error as e:
        raise Exception(f"SQL error reading data from {actual_base_table_name}: {e}")

    if not all_data:
        print(f"  Table {actual_base_table_name} has no data.");
        return

    partition_data_map = {i: [] for i in range(N)}
    for idx, record in enumerate(all_data):
        partition_data_map[idx % N].append(record)

    del all_data
    import gc
    gc.collect()

    insert_chunk_size_rr = 50000
    try:
        with conn_rr_part.cursor() as cur_insert:
            for i in range(N):
                part_table_name = f"{RROBIN_TABLE_PREFIX}{i}"
                records = partition_data_map[i]
                if records:
                    psycopg2.extras.execute_values(
                        cur_insert,
                        f'INSERT INTO {part_table_name} (userid, movieid, rating) VALUES %s',
                        records, page_size=insert_chunk_size_rr)
            if not conn_rr_part.autocommit:
                conn_rr_part.commit()
        print(f"🎉 Round Robin partitioning successful!");
    except psycopg2.Error as e:
        if not conn_rr_part.autocommit and conn_rr_part.status == psycopg2.extensions.STATUS_IN_ERROR:
            conn_rr_part.rollback()
        raise Exception(f"SQL error inserting into round-robin partitions: {e}")
    finally:
        del partition_data_map
        gc.collect()


def get_number_of_partitions_pg_with_conn(conn, prefix_to_match):
    if not conn or conn.closed:
        print("🚫 Error get_number_of_partitions: Invalid connection.")
        return 0

    like_pattern = f"{prefix_to_match}%"

    query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE %s;"
    count = 0
    try:
        with conn.cursor() as cur:
            cur.execute(query, (like_pattern,))
            result = cur.fetchone()
            if result and result[0] is not None:
                count = result[0]
    except psycopg2.Error as e:
        print(f"SQL error in get_number_of_partitions_pg_with_conn (pattern: {like_pattern}): {e}")
    return count


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    actual_base_table_name = ratingstablename.lower()
    MovieID = itemid
    RatingVal = float(rating)
    UserID = userid
    print(f"⏳ Round Robin Insert (PostgreSQL): UserID={UserID}, MovieID={MovieID}, Rating={RatingVal}")

    conn_rr_insert = openconnection
    if not conn_rr_insert or conn_rr_insert.closed:
        raise Exception("roundrobininsert: Invalid PostgreSQL connection.")

    insert_original_sql = f'INSERT INTO {actual_base_table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
    _execute_query_pg_with_provided_conn(conn_rr_insert, insert_original_sql, (UserID, MovieID, RatingVal))
    print(f"  👍 Inserted into original table {actual_base_table_name}.")

    N = get_number_of_partitions_pg_with_conn(conn_rr_insert, RROBIN_TABLE_PREFIX)

    if N == 0:
        print("⚠️ No round-robin partitions found.");
        return

    total_records_in_all_rr_partitions = 0
    try:
        with conn_rr_insert.cursor() as cur_count:
            for i in range(N):
                part_name = f"{RROBIN_TABLE_PREFIX}{i}"
                cur_count.execute(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{part_name}');")
                if cur_count.fetchone()[0]:
                    cur_count.execute(f'SELECT COUNT(*) FROM {part_name};')
                    res = cur_count.fetchone()
                    if res and res[0] is not None:
                        total_records_in_all_rr_partitions += res[0]
                else:
                    print(f"  INFO (roundrobininsert): Partition table {part_name} not found during count. Skipping.")

    except psycopg2.Error as e:
        if not conn_rr_insert.autocommit and hasattr(conn_rr_insert,
                                                     'transaction_status') and conn_rr_insert.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
            conn_rr_insert.rollback()
        raise Exception(f"SQL error counting records in RR partition: {e}")

    target_idx = total_records_in_all_rr_partitions % N
    target_part_name = f"{RROBIN_TABLE_PREFIX}{target_idx}"

    print(
        f"  Total existing RR records: {total_records_in_all_rr_partitions}. New record will go into partition index {target_idx} ({target_part_name}).")

    insert_part_sql = f'INSERT INTO {target_part_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
    try:
        _execute_query_pg_with_provided_conn(conn_rr_insert, insert_part_sql, (UserID, MovieID, RatingVal))
        print(f"  ✅ Successfully inserted into partition {target_part_name}.")
    except psycopg2.Error as e:
        print(f"🚫 Error inserting into partition {target_part_name}: {e}")
        with conn_rr_insert.cursor() as cur_check:
            cur_check.execute(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{target_part_name}');")
            if not cur_check.fetchone()[0]:
                print(f"  ERROR: Target partition table {target_part_name} does not exist. Insertion failed.")
        raise

# --- Main Execution Block (for standalone running) ---
if __name__ == "__main__":
    print("🚀 main.py (PostgreSQL - Revised) running standalone...")
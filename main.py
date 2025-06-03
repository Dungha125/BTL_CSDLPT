import psycopg2
import psycopg2.extras  # Needed for execute_values for bulk inserts

# --- Constants ---
DATABASE_NAME = 'postgres'  # Default database name for initial connection
DB_USER_PG_DEFAULT = 'postgres'
DB_PASS_PG_DEFAULT = '12052004'
DB_HOST_PG_DEFAULT = 'localhost'
DB_PORT_PG_DEFAULT = '5432'  # Default PostgreSQL port

# Prefixes for partitioned tables
RANGE_TABLE_PREFIX = "range_part"
RROBIN_TABLE_PREFIX = "rrobin_part"

# Column names for consistency (ensuring lowercase for PostgreSQL default)
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# Constants for Range Partitioning boundaries
MIN_RATING_CONST = 0.0
MAX_RATING_CONST = 5.0


# --- Helper Functions (blending from Code 2's robustness) ---

def getopenconnection(user=DB_USER_PG_DEFAULT, password=DB_PASS_PG_DEFAULT, dbname=DATABASE_NAME,
                      host=DB_HOST_PG_DEFAULT, port=DB_PORT_PG_DEFAULT):
    """
    Establishes and returns a connection to the PostgreSQL database.
    Handles connection errors.
    """
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        # Note: By default, psycopg2 connections are not in autocommit mode.
        # For an assignment, the test harness might set autocommit, or you'll need
        # explicit commits (which are included in the main functions).
        return conn
    except psycopg2.Error as e:
        print(f"🚫 Error connecting to PostgreSQL: {e}")
        raise  # Re-raise to let the caller handle connection failures


def getopenconnection(user=DB_USER_PG_DEFAULT, password=DB_PASS_PG_DEFAULT, dbname=DATABASE_NAME, host=DB_HOST_PG_DEFAULT, port=DB_PORT_PG_DEFAULT):
    """
    Thiết lập và trả về một kết nối đến cơ sở dữ liệu PostgreSQL.
    """
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        return conn
    except psycopg2.Error as e:
        print(f"🚫 Lỗi kết nối đến PostgreSQL: {e}")
        raise

def _execute_query_pg_with_provided_conn(conn, query, params=None, fetch=False):
    """
    Thực thi một truy vấn SQL bằng cách sử dụng kết nối được cung cấp.
    Xử lý ghi lỗi cơ bản và lấy kết quả.
    """
    if not conn or conn.closed:
        raise psycopg2.InterfaceError("Kết nối không hợp lệ hoặc đã đóng trong _execute_query_pg_with_provided_conn.")
    result = None
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch == 'one':
                result = cur.fetchone()
            elif fetch == 'all':
                result = cur.fetchall()
    except psycopg2.Error as e:
        print(f"🚫 Lỗi SQL (PostgreSQL) trong _execute_query_pg_with_provided_conn: {e}")
        print(f"  Truy vấn thất bại: {query}")
        if params: print(f"  Tham số: {params}")
        # Thử rollback nếu không ở chế độ autocommit và lỗi xảy ra
        if not conn.autocommit and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            print("  INFO: Đang cố gắng rollback do lỗi (không ở chế độ autocommit)...")
            try:
                conn.rollback()
            except psycopg2.Error as rb_err:
                print(f"  Lỗi trong quá trình rollback: {rb_err}")
        raise # Ném lại ngoại lệ
    return result

def create_db_if_not_exists(dbname):
    """
    Kết nối đến cơ sở dữ liệu PostgreSQL mặc định và tạo một cơ sở dữ liệu mới
    nếu nó chưa tồn tại.
    """
    print(f"⏳ Đảm bảo cơ sở dữ liệu '{dbname}' tồn tại...")
    conn_default = None
    try:
        conn_default = getopenconnection(dbname='postgres')
        conn_default.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn_default.cursor()

        cur.execute('SELECT 1 FROM pg_database WHERE datname = %s;', (dbname,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f'CREATE DATABASE {psycopg2.extensions.quote_ident(dbname, cur)}')
            print(f"✅ Cơ sở dữ liệu '{dbname}' đã được tạo thành công.")
        else:
            print(f"➡️ Cơ sở dữ liệu '{dbname}' đã tồn tại. Bỏ qua việc tạo.")
    except psycopg2.Error as e:
        print(f"🚫 Lỗi khi tạo hoặc kiểm tra cơ sở dữ liệu '{dbname}': {e}")
        raise
    finally:
        if conn_default:
            conn_default.close()

def _count_partitions_with_prefix(openconnection, prefix_to_match):
    """
    Đếm số lượng bảng trong schema 'public' khớp với tiền tố đã cho.
    """
    conn = openconnection
    if not conn or conn.closed:
        print("🚫 Lỗi trong _count_partitions_with_prefix: Kết nối không hợp lệ.")
        return 0

    query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE %s;"
    try:
        count = _execute_query_pg_with_provided_conn(conn, query, (f"{prefix_to_match}%",), fetch='one')[0]
    except psycopg2.Error as e:
        print(f"🚫 Lỗi SQL khi đếm phân vùng (tiền tố: '{prefix_to_match}%'): {e}")
        count = 0
    return count

# --- Hàm chính (pha trộn Code 1 và Code 2) ---

def create_partition_table(cursor, table_name):
    """
    Hàm hỗ trợ để tạo một bảng phân vùng mới với schema tiêu chuẩn.
    Được sử dụng bởi Range_Partition và RoundRobin_Partition.
    """
    sql_table_name = table_name.lower()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {sql_table_name} (
            {USER_ID_COLNAME} INT,
            {MOVIE_ID_COLNAME} INT,
            {RATING_COLNAME} FLOAT
        );
    ''')

def loadratings(ratingsTableName, ratingsFilePath, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {ratingsTableName} (
                UserID INT,
                MovieID INT,
                Rating FLOAT
            );
        ''')
        with open(ratingsFilePath, 'r') as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) < 3: continue
                userid, movieid, rating = int(parts[0]), int(parts[1]), float(parts[2])
                cur.execute(f'''
                    INSERT INTO {ratingsTableName} (UserID, MovieID, Rating)
                    VALUES (%s, %s, %s);
                ''', (userid, movieid, rating))
        openconnection.commit()


def rangepartition(ratingsTableName, numberOfPartitions, openconnection):
    """
    Function to create partitions of the main table based on range of ratings.
    """
    actual_base_table_name = ratingsTableName.lower()
    n = numberOfPartitions

    if n <= 0:
        raise ValueError("numberOfPartitions must be greater than 0 for range partitioning.")

    print(f"⏳ Partitioning table '{actual_base_table_name}' into {n} range partitions (PostgreSQL)...")

    conn = openconnection
    if not conn or conn.closed:
        raise Exception("Range_Partition: Invalid or closed database connection provided.")

    range_step = (MAX_RATING_CONST - MIN_RATING_CONST) / n
    if range_step == 0 and n > 0:
        raise ValueError("Range step is zero. Check MIN_RATING_CONST, MAX_RATING_CONST, or numberOfPartitions.")

    previous_upper_bound = MIN_RATING_CONST

    try:
        with conn.cursor() as cur:
            for i in range(n):
                table_name = f"{RANGE_TABLE_PREFIX}{i}"

                # Drop existing table to ensure a clean start
                _execute_query_pg_with_provided_conn(conn, f"DROP TABLE IF EXISTS {table_name} CASCADE;")

                # Create the partition table
                create_partition_table(cur, table_name)

                current_upper = MIN_RATING_CONST + (i + 1) * range_step
                if i == n - 1:  # Ensure last partition includes MAX_RATING_CONST
                    current_upper = MAX_RATING_CONST

                condition = ""
                if i == 0:
                    condition = f"{RATING_COLNAME} >= %s AND {RATING_COLNAME} <= %s"
                    params = (MIN_RATING_CONST, current_upper)
                else:
                    condition = f"{RATING_COLNAME} > %s AND {RATING_COLNAME} <= %s"
                    params = (previous_upper_bound, current_upper)

                insert_sql = f"""
                INSERT INTO {table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
                SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME} FROM {actual_base_table_name}
                WHERE {condition};
                """
                _execute_query_pg_with_provided_conn(conn, insert_sql, params)

                previous_upper_bound = current_upper
                print(
                    f"  ✅ Created and populated partition '{table_name}' (Ratings: {previous_upper_bound - range_step if i > 0 else MIN_RATING_CONST:.2f} to {current_upper:.2f}).")

            if not conn.autocommit:
                conn.commit()
        print(f"🎉 Range partitioning completed successfully! {n} partitions created.")
    except Exception as e:
        print(f"🚫 Error during Range_Partition: {e}")
        if not conn.autocommit and conn and not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise


def roundrobinpartition(ratingsTableName, numberOfPartitions, openconnection):
    """
    Function to create partitions of the main table using a round-robin approach.
    Each record from the main table is distributed to a partition based on its row number modulo N.
    """
    cursor = openconnection.cursor()

    # Xóa và tạo lại các bảng phân mảnh vòng tròn
    for i in range(numberOfPartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cursor.execute("DROP TABLE IF EXISTS " + table_name)
        cursor.execute("CREATE TABLE " + table_name + " (UserID INT, MovieID INT, Rating FLOAT);")

    # Lấy tất cả dữ liệu từ bảng gốc
    cursor.execute("SELECT UserID, MovieID, Rating FROM " + ratingsTableName)
    rows = cursor.fetchall()

    # Chia dữ liệu vào các phân mảnh theo vòng tròn
    for index, row in enumerate(rows):
        target_partition = index % numberOfPartitions
        target_table = RROBIN_TABLE_PREFIX + str(target_partition)
        cursor.execute("INSERT INTO " + target_table + " (UserID, MovieID, Rating) VALUES (%s, %s, %s)", row)

    openconnection.commit()


def rangeinsert(ratingsTableName, userid, movieid, rating, openconnection):
    """
    Function to insert a new row into the main table and its corresponding
    range-based partition.
    """
    actual_base_table_name = ratingsTableName.lower()
    MovieID = movieid
    RatingVal = float(rating)
    UserID = userid
    print(f"⏳ Performing Range Insert: UserID={UserID}, MovieID={MovieID}, Rating={RatingVal}")

    conn = openconnection
    if not conn or conn.closed:
        raise Exception("Range_Insert: Invalid or closed database connection provided.")

    try:
        with conn.cursor() as cur:
            # Step 1: Insert into the main table
            insert_original_sql = f'INSERT INTO {actual_base_table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
            _execute_query_pg_with_provided_conn(conn, insert_original_sql, (UserID, MovieID, RatingVal))
            print(f"  👍 Inserted into main table '{actual_base_table_name}'.")

            # Step 2: Determine the target partition based on rating
            num_partitions = _count_partitions_with_prefix(conn, RANGE_TABLE_PREFIX)
            if num_partitions == 0:
                print("⚠️ No range partitions found. Skipping insert into partition.")
                if not conn.autocommit: conn.commit()
                return

            target_part_table_name = None
            if num_partitions == 1:
                target_part_table_name = f"{RANGE_TABLE_PREFIX}0"
                if not (MIN_RATING_CONST <= RatingVal <= MAX_RATING_CONST):
                    print(
                        f"⚠️ Warning: Rating {RatingVal} is outside the defined range [{MIN_RATING_CONST}, {MAX_RATING_CONST}]. Skipping insert into partition.")
                    if not conn.autocommit: conn.commit()
                    return
            else:
                range_step = (MAX_RATING_CONST - MIN_RATING_CONST) / num_partitions
                if range_step == 0:
                    raise ValueError("Range step is zero during Range_Insert. Check partition configuration.")

                determined_index = -1
                current_lower_bound = MIN_RATING_CONST
                for i in range(num_partitions):
                    current_upper_bound = MIN_RATING_CONST + (i + 1) * range_step
                    if i == num_partitions - 1:  # Ensure last partition includes MAX_RATING_CONST
                        current_upper_bound = MAX_RATING_CONST

                    if i == 0:
                        if RatingVal >= current_lower_bound and RatingVal <= current_upper_bound:
                            determined_index = i
                            break
                    else:  # Subsequent partitions use > lower bound to avoid duplicates on boundaries
                        if RatingVal > current_lower_bound and RatingVal <= current_upper_bound:
                            determined_index = i
                            break
                    current_lower_bound = current_upper_bound

                if determined_index != -1:
                    target_part_table_name = f"{RANGE_TABLE_PREFIX}{determined_index}"
                else:
                    print(
                        f"⚠️ Warning: Rating {RatingVal} does not fall into any defined range partition. Skipping insert into partition.")
                    if not conn.autocommit: conn.commit()
                    return

            if target_part_table_name:
                print(f"  🎯 Data will be inserted into partition '{target_part_table_name}' for Rating {RatingVal}.")
                insert_partition_sql = f'INSERT INTO {target_part_table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
                _execute_query_pg_with_provided_conn(conn, insert_partition_sql, (UserID, MovieID, RatingVal))
                print(f"  ✅ Successfully inserted into partition '{target_part_table_name}'.")

            if not conn.autocommit:
                conn.commit()

    except Exception as e:
        print(f"🚫 Error during Range_Insert: {e}")
        if not conn.autocommit and conn and not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise


def roundrobininsert(ratingsTableName, userid, movieid, rating, openconnection):
    cursor = openconnection.cursor()

    # 1. Insert vào bảng chính
    cursor.execute(
        f"INSERT INTO {ratingsTableName} (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
        (userid, movieid, rating)
    )

    # 2. Đếm số lượng phân mảnh Round Robin
    cursor.execute(
        f"SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE '{RROBIN_TABLE_PREFIX.lower()}%';"
    )
    num_partitions = cursor.fetchone()[0]

    if num_partitions == 0:
        openconnection.commit()
        return

    # 3. Tính tổng số bản ghi trong các phân mảnh hiện tại
    total_rows = 0
    for i in range(num_partitions):
        part_table = f"{RROBIN_TABLE_PREFIX}{i}"
        cursor.execute(f"SELECT COUNT(*) FROM {part_table};")
        count = cursor.fetchone()[0]
        total_rows += count

    # 4. Tính index phân mảnh tiếp theo theo round-robin
    next_partition_index = total_rows % num_partitions
    target_partition_table = f"{RROBIN_TABLE_PREFIX}{next_partition_index}"

    # 5. Insert vào phân mảnh
    cursor.execute(
        f"INSERT INTO {target_partition_table} (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
        (userid, movieid, rating)
    )

    # 6. Commit
    openconnection.commit()


# --- Example Usage (for direct testing, similar to Code 2's __main__ block) ---
if __name__ == "__main__":
    print("🚀 Running database partitioning functions (PostgreSQL) directly for testing...")

    # Ensure the database exists
    # try:
    #     create_db_if_not_exists(DATABASE_NAME)
    # except Exception as e:
    #     print(f"Fatal error during database creation: {e}")
    #     exit(1)
    #
    # conn = None
    # try:
    #     # Get a connection for subsequent operations
    #     conn = getopenconnection(dbname=DATABASE_NAME)
    #     # Set to autocommit for easier debugging of individual operations in a script.
    #     # In a real application or test harness, transaction management might differ.
    #     conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    #     print("✅ Connection established for testing.")
    #
    #     test_table_name = "test_ratings_table"
    #     test_file_path = "ml-1m/ratings.dat"  # Adjust path to your ratings.dat file
    #
    #     # Create a dummy ratings.dat for quick test if not available
    #     import os
    #
    #     if not os.path.exists(test_file_path):
    #         print(f"WARNING: '{test_file_path}' not found. Creating a dummy file for testing LoadRatings.")
    #         os.makedirs(os.path.dirname(test_file_path) or '.', exist_ok=True)
    #         with open(test_file_path, 'w') as f:
    #             f.write("1::1::5.0::978300760\n")
    #             f.write("2::1::4.0::978300760\n")
    #             f.write("3::2::3.5::978300760\n")
    #             f.write("4::2::2.0::978300760\n")
    #             f.write("5::3::1.0::978300760\n")
    #             f.write("6::3::5.0::978300760\n")
    #             f.write("7::4::4.5::978300760\n")
    #             f.write("8::4::0.5::978300760\n")
    #             f.write("9::5::3.0::978300760\n")
    #             f.write("10::5::2.5::978300760\n")
    #         print("Dummy 'ratings.dat' created.")
    #
    #     print("\n--- Testing LoadRatings ---")
    #     try:
    #         loaded_count = LoadRatings(test_table_name, test_file_path, conn)
    #         print(f"Loaded {loaded_count} records into '{test_table_name}'.")
    #         count_in_main = \
    #         _execute_query_pg_with_provided_conn(conn, f"SELECT COUNT(*) FROM {test_table_name};", fetch='one')[0]
    #         print(f"Actual count in '{test_table_name}': {count_in_main}")
    #     except Exception as e:
    #         print(f"🚫 Error during LoadRatings test: {e}")
    #
    #     # Clean up existing partitions before creating new ones for consistent testing
    #     print("\n--- Cleaning up existing partitions ---")
    #     try:
    #         cur = conn.cursor()
    #         # Find and drop tables matching our prefixes
    #         cur.execute(
    #             f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND (table_name LIKE '{RANGE_TABLE_PREFIX}%' OR table_name LIKE '{RROBIN_TABLE_PREFIX}%');")
    #         existing_parts = cur.fetchall()
    #         for part in existing_parts:
    #             print(f"  Dropping existing partition: {part[0]}")
    #             cur.execute(f"DROP TABLE IF EXISTS {part[0]} CASCADE;")
    #         conn.commit()
    #         print("✅ Existing partitions cleaned up.")
    #     except Exception as e:
    #         print(f"🚫 Error during partition cleanup: {e}")
    #
    #     print("\n--- Testing Range_Partition ---")
    #     num_range_partitions = 5
    #     try:
    #         Range_Partition(test_table_name, num_range_partitions, conn)
    #         for i in range(num_range_partitions):
    #             part_name = f"{RANGE_TABLE_PREFIX}{i}"
    #             count = _execute_query_pg_with_provided_conn(conn, f"SELECT COUNT(*) FROM {part_name};", fetch='one')[0]
    #             print(f"  Count in '{part_name}': {count} records.")
    #     except Exception as e:
    #         print(f"🚫 Error during Range_Partition test: {e}")
    #
    #     print("\n--- Testing RoundRobin_Partition ---")
    #     num_rrobin_partitions = 3
    #     try:
    #         RoundRobin_Partition(test_table_name, num_rrobin_partitions, conn)
    #         for i in range(num_rrobin_partitions):
    #             part_name = f"{RROBIN_TABLE_PREFIX}{i}"
    #             count = _execute_query_pg_with_provided_conn(conn, f"SELECT COUNT(*) FROM {part_name};", fetch='one')[0]
    #             print(f"  Count in '{part_name}': {count} records.")
    #     except Exception as e:
    #         print(f"🚫 Error during RoundRobin_Partition test: {e}")
    #
    #     print("\n--- Testing Range_Insert ---")
    #     try:
    #         Range_Insert(test_table_name, 100, 1000, 4.2, conn)
    #         Range_Insert(test_table_name, 101, 1001, 0.8, conn)
    #         Range_Insert(test_table_name, 102, 1002, 5.0, conn)  # Max rating
    #         Range_Insert(test_table_name, 103, 1003, 0.0, conn)  # Min rating
    #         # You can add more verification here if needed
    #     except Exception as e:
    #         print(f"🚫 Error during Range_Insert test: {e}")
    #
    #     print("\n--- Testing RoundRobin_Insert ---")
    #     try:
    #         RoundRobin_Insert(test_table_name, 200, 2000, 3.0, conn)
    #         RoundRobin_Insert(test_table_name, 201, 2001, 4.0, conn)
    #         RoundRobin_Insert(test_table_name, 202, 2002, 1.5, conn)
    #         RoundRobin_Insert(test_table_name, 203, 2003, 2.5, conn)
    #         # You can add more verification here if needed
    #     except Exception as e:
    #         print(f"🚫 Error during RoundRobin_Insert test: {e}")
    #
    # except Exception as e:
    #     print(f"🚨 An unhandled error occurred in the main execution block: {e}")
    # finally:
    #     if conn:
    #         conn.close()
    #         print("Connection closed.")
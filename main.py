import psycopg2
import psycopg2.extras  


DATABASE_NAME = 'postgres'  
DB_USER_PG_DEFAULT = 'postgres'
DB_PASS_PG_DEFAULT = '12052004'
DB_HOST_PG_DEFAULT = 'localhost'
DB_PORT_PG_DEFAULT = '5432'  

# Prefixes for partitioned tables
RANGE_TABLE_PREFIX = "range_part"
RROBIN_TABLE_PREFIX = "rrobin_part"


USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# Constants for Range Partitioning boundaries
MIN_RATING_CONST = 0.0
MAX_RATING_CONST = 5.0


def getopenconnection(user=DB_USER_PG_DEFAULT, password=DB_PASS_PG_DEFAULT, dbname=DATABASE_NAME,
                      host=DB_HOST_PG_DEFAULT, port=DB_PORT_PG_DEFAULT):
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        return conn
    except psycopg2.Error as e:
        print(f"🚫 Error connecting to PostgreSQL: {e}")
        raise 


def getopenconnection(user=DB_USER_PG_DEFAULT, password=DB_PASS_PG_DEFAULT, dbname=DATABASE_NAME, host=DB_HOST_PG_DEFAULT, port=DB_PORT_PG_DEFAULT):
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        return conn
    except psycopg2.Error as e:
        print(f"🚫 Lỗi kết nối đến PostgreSQL: {e}")
        raise

def _execute_query_pg_with_provided_conn(conn, query, params=None, fetch=False):
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
        if not conn.autocommit and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            print("  INFO: Đang cố gắng rollback do lỗi (không ở chế độ autocommit)...")
            try:
                conn.rollback()
            except psycopg2.Error as rb_err:
                print(f"  Lỗi trong quá trình rollback: {rb_err}")
        raise 
    return result

def create_db_if_not_exists(dbname):
    print(f"Đảm bảo cơ sở dữ liệu '{dbname}' tồn tại...")
    conn_default = None
    try:
        conn_default = getopenconnection(dbname='postgres')
        conn_default.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn_default.cursor()

        cur.execute('SELECT 1 FROM pg_database WHERE datname = %s;', (dbname,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f'CREATE DATABASE {psycopg2.extensions.quote_ident(dbname, cur)}')
            print(f"Cơ sở dữ liệu '{dbname}' đã được tạo thành công.")
        else:
            print(f"Cơ sở dữ liệu '{dbname}' đã tồn tại. Bỏ qua việc tạo.")
    except psycopg2.Error as e:
        print(f"Lỗi khi tạo hoặc kiểm tra cơ sở dữ liệu '{dbname}': {e}")
        raise
    finally:
        if conn_default:
            conn_default.close()

def _count_partitions_with_prefix(openconnection, prefix_to_match):
    conn = openconnection
    if not conn or conn.closed:
        print("Lỗi trong _count_partitions_with_prefix: Kết nối không hợp lệ.")
        return 0
    query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE %s;"
    try:
        count = _execute_query_pg_with_provided_conn(conn, query, (f"{prefix_to_match}%",), fetch='one')[0]
    except psycopg2.Error as e:
        print(f"Lỗi SQL khi đếm phân vùng (tiền tố: '{prefix_to_match}%'): {e}")
        count = 0
    return count
def create_partition_table(cursor, table_name):
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
    actual_base_table_name = ratingsTableName.lower()
    n = numberOfPartitions
    if n <= 0:
        raise ValueError("numberOfPartitions must be greater than 0 for range partitioning.")
    print(f"Partitioning table '{actual_base_table_name}' into {n} range partitions (PostgreSQL)...")
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
                _execute_query_pg_with_provided_conn(conn, f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                create_partition_table(cur, table_name)
                current_upper = MIN_RATING_CONST + (i + 1) * range_step
                if i == n - 1: 
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
                    f"Created and populated partition '{table_name}' (Ratings: {previous_upper_bound - range_step if i > 0 else MIN_RATING_CONST:.2f} to {current_upper:.2f}).")
            if not conn.autocommit:
                conn.commit()
        print(f"Range partitioning completed successfully! {n} partitions created.")
    except Exception as e:
        print(f"Error during Range_Partition: {e}")
        if not conn.autocommit and conn and not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise
    
def roundrobinpartition(ratingsTableName, numberOfPartitions, openconnection):
    cursor = openconnection.cursor()
    for i in range(numberOfPartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cursor.execute("DROP TABLE IF EXISTS " + table_name)
        cursor.execute("CREATE TABLE " + table_name + " (UserID INT, MovieID INT, Rating FLOAT);")
    cursor.execute("SELECT UserID, MovieID, Rating FROM " + ratingsTableName)
    rows = cursor.fetchall()
    for index, row in enumerate(rows):
        target_partition = index % numberOfPartitions
        target_table = RROBIN_TABLE_PREFIX + str(target_partition)
        cursor.execute("INSERT INTO " + target_table + " (UserID, MovieID, Rating) VALUES (%s, %s, %s)", row)
    openconnection.commit()

def rangeinsert(ratingsTableName, userid, movieid, rating, openconnection):
    actual_base_table_name = ratingsTableName.lower()
    MovieID = movieid
    RatingVal = float(rating)
    UserID = userid
    print(f"Performing Range Insert: UserID={UserID}, MovieID={MovieID}, Rating={RatingVal}")
    conn = openconnection
    if not conn or conn.closed:
        raise Exception("Range_Insert: Invalid or closed database connection provided.")
    try:
        with conn.cursor() as cur:
            insert_original_sql = f'INSERT INTO {actual_base_table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
            _execute_query_pg_with_provided_conn(conn, insert_original_sql, (UserID, MovieID, RatingVal))
            print(f"Inserted into main table '{actual_base_table_name}'.")
            num_partitions = _count_partitions_with_prefix(conn, RANGE_TABLE_PREFIX)
            if num_partitions == 0:
                print("No range partitions found. Skipping insert into partition.")
                if not conn.autocommit: conn.commit()
                return
            target_part_table_name = None
            if num_partitions == 1:
                target_part_table_name = f"{RANGE_TABLE_PREFIX}0"
                if not (MIN_RATING_CONST <= RatingVal <= MAX_RATING_CONST):
                    print(
                        f"Warning: Rating {RatingVal} is outside the defined range [{MIN_RATING_CONST}, {MAX_RATING_CONST}]. Skipping insert into partition.")
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
                    if i == num_partitions - 1:  
                        current_upper_bound = MAX_RATING_CONST
                    if i == 0:
                        if RatingVal >= current_lower_bound and RatingVal <= current_upper_bound:
                            determined_index = i
                            break
                    else: 
                        if RatingVal > current_lower_bound and RatingVal <= current_upper_bound:
                            determined_index = i
                            break
                    current_lower_bound = current_upper_bound
                if determined_index != -1:
                    target_part_table_name = f"{RANGE_TABLE_PREFIX}{determined_index}"
                else:
                    print(
                        f"Warning: Rating {RatingVal} does not fall into any defined range partition. Skipping insert into partition.")
                    if not conn.autocommit: conn.commit()
                    return
            if target_part_table_name:
                print(f"Data will be inserted into partition '{target_part_table_name}' for Rating {RatingVal}.")
                insert_partition_sql = f'INSERT INTO {target_part_table_name} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES (%s, %s, %s);'
                _execute_query_pg_with_provided_conn(conn, insert_partition_sql, (UserID, MovieID, RatingVal))
                print(f"Successfully inserted into partition '{target_part_table_name}'.")

            if not conn.autocommit:
                conn.commit()
    except Exception as e:
        print(f"Error during Range_Insert: {e}")
        if not conn.autocommit and conn and not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_ERROR:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise


def roundrobininsert(ratingsTableName, userid, movieid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute(
        f"INSERT INTO {ratingsTableName} (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
        (userid, movieid, rating)
    )
    cursor.execute(
        f"SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE '{RROBIN_TABLE_PREFIX.lower()}%';"
    )
    num_partitions = cursor.fetchone()[0]
    if num_partitions == 0:
        openconnection.commit()
        return
    total_rows = 0
    for i in range(num_partitions):
        part_table = f"{RROBIN_TABLE_PREFIX}{i}"
        cursor.execute(f"SELECT COUNT(*) FROM {part_table};")
        count = cursor.fetchone()[0]
        total_rows += count
    next_partition_index = total_rows % num_partitions
    target_partition_table = f"{RROBIN_TABLE_PREFIX}{next_partition_index}"
    cursor.execute(
        f"INSERT INTO {target_partition_table} (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
        (userid, movieid, rating)
    )
    openconnection.commit()


if __name__ == "__main__":
    print("Running database partitioning functions directly for testing...")


import psycopg2
import psycopg2.extras

def get_coonection():

    con = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="admin",
    host="localhost",
    port= '5432'
    )
    return con


""" Get raw value from weather data from postgres"""
def get_raw_weather_data(tablename, skip, limit, station_id=None, filter_date=None):


        conn = get_coonection()
        cur_obj = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        rows = None
        
        
        table_name = 'wx_data_table '
        
        limit_clause = " limit %s offset %s"

        if station_id and not filter_date:
            where_clause = "where station=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            cur_obj.execute(sql, (station_id, limit, skip))

        elif filter_date and not station_id:
            where_clause = "where record_date=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            cur_obj.execute(sql, (filter_date, limit, skip))
        elif station_id and filter_date:

            where_clause = "where station=%s and record_date=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            print(sql)
            cur_obj.execute(sql, (station_id, filter_date, limit, skip))

        else:
            sql = "select * from " + table_name + limit_clause
            cur_obj.execute(sql, (limit, skip))

        
        rows = cur_obj.fetchall()
        cur_obj.close()
        conn.close()
        
        return  rows
    

""" Get Aggregate value from weather data from postgres"""
def get_aggregate_weather_data(skip, limit, station_id=None, filter_year=None):


        conn = get_coonection()
        cur_obj = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        rows = None
        
        table_name = 'aggregated_wx_data '
        limit_clause = " limit %s offset %s"

        if station_id and not filter_year:
            where_clause = "where station=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            cur_obj.execute(sql, (station_id, limit, skip))

        elif filter_year and not station_id:
            where_clause = "where record_year=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            cur_obj.execute(sql, (filter_year, limit, skip))
        elif station_id and filter_year:

            where_clause = "where station=%s and record_year=%s" 
            sql = "select * from " + table_name + where_clause + limit_clause
            print(sql)
            cur_obj.execute(sql, (station_id, filter_year, limit, skip))

        else:
            sql = "select * from " + table_name + limit_clause
            cur_obj.execute(sql, (limit, skip))

        
        rows = cur_obj.fetchall()
        cur_obj.close()
        conn.close()
        
        return  rows
    
import psycopg2

def get_coonection():

    con = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="admin",
    host="localhost",
    port= '5432'
    )
    return con

def insert_wx_data(input_list):

    if len(input_list) > 0:

        
        cursor = get_coonection()
        cur_obj = cursor.cursor()
        args_str = b','.join(cur_obj.mogrify("(%s,%s,%s,%s,%s)", x) for x in input_list)
        cur_obj.execute("INSERT INTO wx_data_table VALUES " + args_str.decode() + "on conflict (station, record_date) do nothing") 

        cursor.commit()
        
        rowcount = cur_obj.rowcount

        cur_obj.close()
        #print("row inserted in insert ", rowcount)
        
        return  rowcount
    
def insert_yld_data(input_list):

    if len(input_list) > 0:

        
        cursor = get_coonection()
        cur_obj = cursor.cursor()
        args_str = b','.join(cur_obj.mogrify("(%s,%s)", x) for x in input_list)
        cur_obj.execute("INSERT INTO yeild_data_table VALUES " + args_str.decode() + "on conflict (yeild_year) do nothing") 

        cursor.commit()
        
        rowcount = cur_obj.rowcount
        cur_obj.close()
        #print("row inserted in insert ", rowcount)
        
        return rowcount
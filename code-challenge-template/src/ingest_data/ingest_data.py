import os 
from utility import insert_wx_data
from utility import insert_yld_data
import time


def wx_data_processor():

    rowcount = 0

    path = os.path.realpath(__file__)
    dir = os.path.dirname(path)
    wx_dir = dir.replace('src\ingest_data', 'wx_data')
    
    for file in os.listdir(os.path.abspath(wx_dir)):

        new_list = []
        if file.endswith(".txt"):
            with open(wx_dir + "\\" + file, 'r') as fin:
                lines = list(fin)
                    
                
                for row in lines:
                
                    cols = row.split('\t')
                    station_name = file.split('.')[0]
                    max_temp = cols[1].lstrip()
                    min_temp = cols[2].lstrip()
                    record_date = cols[0]
                    precipitation_val = cols[3].lstrip()

                    if "\n" in precipitation_val:

                        precipitation_val = precipitation_val.replace("\n", "")
                    #date_obj = datetime.datetime.strptime('24052010', '%d%m%Y').date()
                    
                    
                    row_tup = (station_name, record_date, max_temp, min_temp, precipitation_val)
                    new_list.append(row_tup)

      
            new_row = insert_wx_data(new_list)
            #print(file, "  row inserted ", new_row)
            if new_row:
                rowcount = rowcount + new_row
            
    return rowcount

def yld_data_processor():
    
    rowcount = 0
    
    path = os.path.realpath(__file__)
    dir = os.path.dirname(path)
    
    yld_dir = dir.replace('src\ingest_data', 'yld_data')
    
    for file in os.listdir(os.path.abspath(yld_dir)):

        print()
        
        new_list = []
        if file.endswith(".txt"):
            with open(yld_dir + "\\" + file, 'r') as fin:
                lines = list(fin)
     
                for row in lines:
                    cols = row.split('\t')
                    yield_year = cols[0]
                    yeild_value = cols[1].lstrip()
     
                    if "\n" in yeild_value:

                        yeild_value = yeild_value.replace("\n", "")
                    
                    row_tup = (yield_year, yeild_value)
                    new_list.append(row_tup)
        
            new_row = insert_yld_data(new_list)
            if new_row:
                rowcount = rowcount + new_row
        
    return rowcount

def main():
    
    print("\ninsert yeild data")
    rows = yld_data_processor()
    print("Total row inserted for yeild data ", rows)

    print("insert weather data")
    rows = wx_data_processor()
    print("Total row inserted for weather data ", rows)

    


if __name__ == "__main__":

    start_time = time.time()
    print("Start Time :", start_time)

    main()

    print("\nEnd Time :", time.time())
    print("\nTotal execution time : %s seconds", (time.time() - start_time))
import psycopg2
import psycopg2.extras
from utility import get_raw_weather_data
from utility import get_aggregate_weather_data

from fastapi import FastAPI

app = FastAPI()


@app.get("/api/weather/")
async def read_raw_weather_details(skip: int = 0, limit: int = 10, filter_date: str | None = None, station_id: str | None = None):


    response = get_raw_weather_data("raw", skip, limit, station_id, filter_date )
    
    return response

@app.get("/api/weather/stats/")
async def read_aggregated_weather_details( skip: int = 0, limit: int = 10, filter_year: str | None = None, station_id: str | None = None):

    response = get_aggregate_weather_data( skip, limit, station_id, filter_year )
    
    return response

  


def get_coonection():

    
    #config = configparser.ConfigParser()
    #config.read('database.ini')

    #ini_path = os.path.join(os.getcwd(),'database.ini')
    
    con = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="admin",
    host="localhost",
    port= '5432'
    )
    return con
'''
cursor = get_coonection()
cur_obj = cursor.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
#args_str = b','.join(cur_obj.mogrify("(%s,%s,%s,%s,%s)", x) for x in input_list)
cur_obj.execute("Select * from avg_min_temp_per_year Limit 5") 


res = cur_obj.fetchall()

print(res)

cur_obj.close()
cursor.close()
#print("row inserted in insert ", rowcount)
'''

# response = get_raw_data(skip=0, limit=10, filter_date=None, station_id=None)
# #print(response)
# response = get_raw_data(skip=0, limit=10, filter_date='1985-01-01', station_id=None)
# #print(response)
# #response = get_raw_data(skip=0, limit=10, filter_date='1985-01-01', station_id='USC00110072')
# print(response)
# response = get_raw_data(skip=0, limit=10, filter_date='1985-01-01', station_id='USC00110072')
#print(response)

# json = json.dumps(response, indent = 4, default=str)
# print(response)
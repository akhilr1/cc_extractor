from fastapi import FastAPI
# from fastapi import Body
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from .config import settings
from .cc_engine import common_crawl_index_list,wet_extractor,common_crawl_index,upload_to_es
# from elastic_transport import ConnectionTimeout
# from es_pandas import es_pandas
import pandas as pd




app = FastAPI()

class cc_input(BaseModel):
    domain: str
    year: int

try:
    conn= psycopg2.connect(host=settings.db_host ,database=settings.db_database ,user=settings.db_username ,password=settings.db_password , cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    print('DB connection successfull')

except Exception as error:
    print('DB connection failed')
    print(error)
    time.sleep(5)



@app.get("/")
def root():
    return {"message": "Hello"}

@app.post("/domain")
def extract_by_domain(input: cc_input):
    input_dict = input.model_dump()
    year = input_dict['year']
    domain = input_dict['domain']
    
    index_list = common_crawl_index_list(year)
    if index_list:
        res=[]
        for index in index_list:
            cursor.execute(
                """SELECT * FROM index_list WHERE cdx_api = %s AND domain = %s""",
                (index, domain),
            )
            index_exists = cursor.fetchone()
            if index_exists:
                print(index_exists)
            else:
                call_count = 0
                while call_count <= 10:
                    call_count += 1
                    index_results = common_crawl_index(domain, index)
                    print(index_results)
                    if index_results:
                        break

                warc_filename_list = [items['filename'] for items in index_results]
                for filename in warc_filename_list:
                    cursor.execute(
                        """SELECT * FROM cc_wetlist WHERE filename = %s""", (filename,)
                    )
                    file_exists = cursor.fetchone()
                    if not file_exists:
                        result = wet_extractor(filename)
                        if result is not None:
                            wet_data = pd.DataFrame(result)
                            print(wet_data)
                            es_success = upload_to_es(wet_data)
                            if es_success:
                                cursor.execute(
                                    """INSERT INTO cc_wetlist (filename, domain, status) VALUES (%s, %s, %s) RETURNING *""",
                                    (filename, domain, 1),
                                )
                                wet_res = cursor.fetchone()
                                conn.commit()
                                if wet_res:
                                    print(wet_res)

                                    file={'filename':wet_res}
                                    res.append(file)
                                
                if cursor:
                    cursor.execute(
                        """INSERT INTO index_list (cdx_api, domain, status) VALUES (%s, %s, %s) RETURNING *""",
                        (index, domain, 1),
                    )
                    # res = cursor.fetchone()
                    conn.commit()
                else:
                    print("Db not connected")

    if res:
        return {"index": res}
    else:
        return {"message": "Already processed"}
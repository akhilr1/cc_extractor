from warcio import ArchiveIterator
from io import BytesIO
import pandas as pd
import requests
import es_pandas
import json
from es_pandas import es_pandas
from elastic_transport import ConnectionTimeout
from time import sleep
from elasticsearch.helpers import BulkIndexError
from calendar import month_name
import psycopg2
from psycopg2.extras import RealDictCursor
from .config import settings
import logging
# import yaml


# with open('commoncrawl/config.yml','r') as f:
#     settings=yaml.safe_load(f)

logging.basicConfig(
    level = logging.DEBUG,
    filename= "file.log",
    format = "%(asctime)s %(levelname)s :%(name)s :%(lineno)d : %(message)s ",
    datefmt= "%d-%b-%y %H:%M:%S"
)

try:
    conn= psycopg2.connect(host=settings.db_host ,database=settings.db_database ,user=settings.db_username ,password=settings.db_password , cursor_factory=RealDictCursor)

    # conn= psycopg2.connect(host=settings['DB_HOST'] ,database=settings['DB_DATABASE'] ,user=settings['DB_USERNAME'] ,password=settings['DB_PASSWORD'] , cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    logging.info('DB connection successfull')

except Exception as error:
    logging.critical('DB connection failed')
    logging.critical(error)
    sleep(5)


def common_crawl_index_list(year: int):
    index_api_url = "http://index.commoncrawl.org/collinfo.json"

    # Parameters to request crawl segment information
    params = {
        "output": "json"  # Request JSON format response
    }

    # Make the API request
    response = requests.get(index_api_url, params=params)
    filtered_urls = []

    # Check if the request was successful
    if response.status_code == 200:
        crawl_segments = response.json()
        crawl_index= [i['cdx-api'] for i in crawl_segments]
        for url in crawl_index:
            parts = url.split('-')
            cc_year = int(parts[2])
            if cc_year == year:
                filtered_urls.append(url)

        logging.info(f"filtered urls: {filtered_urls}")
        return filtered_urls

    else:
        logging.critical("Error: Unable to retrieve crawl segments")
        return None


def common_crawl_index(domain,index):
    # index_url = f"http://index.commoncrawl.org/CC-MAIN-{index}-index"
    query_url = f"{index}?url={domain}&output=json"
    logging.info(query_url)

    response = requests.get(query_url)
    if response.status_code == 200:
        json_array=[]
        
        for line in response.iter_lines(decode_unicode=True):
            try:
                data = json.loads(line)
                # Process the JSON data here
                json_array.append(data)
            except json.JSONDecodeError:
                # Handle any decoding errors
                pass
        return json_array
    else:
        return False


def wet_extractor(warc_filename):
    wet_filename= warc_filename.replace("/warc/","/wet/").replace("/crawldiagnostics/","/wet/").replace("warc.gz","warc.wet.gz")
    wet_file= f"https://data.commoncrawl.org/{wet_filename}"
    logging.info(wet_file)

    # wet_file = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296949644.27/wet/CC-MAIN-20230331144941-20230331174941-00264.warc.wet.gz"
    
    dataList=[]
    try:
        response = requests.get(wet_file)
        response.raise_for_status() 
        warc_content = BytesIO(response.content)
        for record in ArchiveIterator(warc_content):
            # Access record headers and content here
            
            if(record.rec_headers.get_header('WARC-Identified-Content-Language') and record.rec_headers.get_header('WARC-Identified-Content-Language')=='eng' and int(record.rec_headers.get_header('Content-Length'))>1000  ):
                content = record.content_stream().read().decode('utf-8', errors='replace')
                # soup = BeautifulSoup(content, 'lxml')  # Use the lxml parser
                
                # # Extract text from <p> tags
                # paragraphs = [p.get_text() for p in soup.find_all('p')]
                
                # if paragraphs:
                data = {
                    'warc_target_uri': record.rec_headers.get_header('WARC-Target-URI'),
                    'warc_date': record.rec_headers.get_header('WARC-Date'),
                    'warc_length': record.rec_headers.get_header('Content-Length'),
    
                    'content': content
                }
                dataList.append(data)
        
                
        df = pd.DataFrame(dataList)
        if not df.empty:
            return df
        else:
            return None
    
    except requests.exceptions.HTTPError as e:
        logging.critical(f"filename :{wet_filename}: http error: {e}")
        return None
    except Exception as e:
        logging.critical(f"filename :{wet_filename}: error: {e}")
        return None
    
def cc_worker(domain:str,year:int):
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
                logging.warning(f"{index_exists} : Already processed")
            else:
                call_count = 0
                while call_count <= 10:
                    call_count += 1
                    index_results = common_crawl_index(domain, index)
                    # print(index_results)
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
                            # print(wet_data)
                            es_success = upload_to_es(wet_data)
                            if es_success:
                                cursor.execute(
                                    """INSERT INTO cc_wetlist (filename, domain, status) VALUES (%s, %s, %s) RETURNING *""",
                                    (filename, domain, 1),
                                )
                                wet_res = cursor.fetchone()
                                conn.commit()
                                if wet_res:
                                    # print(wet_res)

                                    file={'filename':wet_res}
                                    res.append(file['filename'])
                                    logging.info(f"{filename} : wet file processed successfully")
                                
                if cursor:
                    cursor.execute(
                        """INSERT INTO index_list (cdx_api, domain, status) VALUES (%s, %s, %s) RETURNING *""",
                        (index, domain, 1),
                    )
                    # res = cursor.fetchone()
                    conn.commit()
                    logging.info(f"{index} :Index processed successfully")

                else:
                    logging.critical("Db not connected")
    if res:
        return {"index": res}
    else:
        return {"message": "Already processed"}



# def monthwise_splitter(df):
#     df['warc_date'] = pd.to_datetime(df['warc_date'])  # Convert to datetime
#     # Split DataFrame into multiple DataFrames based on year and month
#     dfs_by_year_month = {}
#     for name, group in df.groupby([df['warc_date'].dt.year, df['warc_date'].dt.month]):
#         year,month = name
#         dfs_by_year_month.setdefault(year, {}).setdefault(month, group)

#     # Print each DataFrame by year and month
#     for year, year_dfs in dfs_by_year_month.items():
#         for month, df_month in year_dfs.items():
#             # print(f"Year: {year}, Month: {month}")
#             es_name=f"cc_{year}_{month_name[month]}"
#             upload=upload_to_es(df_month,es_name)
#             if not upload:
#                 return False
#     return True


def upload_to_es(data_frame):
    wet_date=data_frame.loc[0]['warc_date']
    date_array= wet_date.split('-')
    es_name=f"cc_{date_array[0]}_{month_name[int(date_array[1])].lower()}"


    max_retries = 10
    retry_delay = 5


    for attempt in range(max_retries):
        try:
            es_host = "http://3.108.185.168:9201"
            ep = es_pandas(es_host, timeout=30)
            index = es_name
            ep.to_es(
                data_frame, index, doc_type="_doc", show_progress=True, thread_count=2, chunk_size=10000
            )
            return True
        except BulkIndexError as e:
            # for error in e.errors:
                logging.critical(e.errors[0])
        except ConnectionTimeout:
            if attempt < max_retries - 1:
                logging.warning(f"Connection timeout. Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
            else:
                logging.critical("Max retries reached. Unable to establish connection.")
                return False

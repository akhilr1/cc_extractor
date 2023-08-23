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
        # Print the entire response content
        # print(crawl_index)
        for url in crawl_index:
            parts = url.split('-')
            cc_year = int(parts[2])
            # print(cc_year)
            if cc_year == year:
                filtered_urls.append(url)

            # print(filtered_urls)
        return filtered_urls

    else:
        print("Error: Unable to retrieve crawl segments")
        return None


def common_crawl_index(domain,index):
    # index_url = f"http://index.commoncrawl.org/CC-MAIN-{index}-index"
    query_url = f"{index}?url={domain}&output=json"
    print(query_url)

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
        print(response.status_code)
        return json_array
    else:
        return False


def wet_extractor(warc_filename):
    wet_filename= warc_filename.replace("/warc/","/wet/").replace("/crawldiagnostics/","/wet/").replace("warc.gz","warc.wet.gz")
    wet_file= f"https://data.commoncrawl.org/{wet_filename}"
    print(wet_file)

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
        # print(df)
    
    except requests.exceptions.HTTPError as e:
        print(f"filename :{wet_filename}: http error: {e}")
        return None
    except Exception as e:
        print(f"filename :{wet_filename}: error: {e}")
        return None
    

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


    max_retries = 5
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
                print(e.errors[0])
        except ConnectionTimeout:
            if attempt < max_retries - 1:
                print(f"Connection timeout. Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
            else:
                print("Max retries reached. Unable to establish connection.")
                return False
# Example usage:
# domain = "wipro.com"
# indexYear = 2021
# indexlist=common_crawl_indexList(indexYear)
# index = "https://index.commoncrawl.org/CC-MAIN-2023-06-index"
# if indexlist is not None:
#     for index in indexlist:
#         call_count=0
#         while True:
#             call_count+=1  
#             index_results = common_crawl_index(domain,index)
#             print(index_results)
#             if index_results or call_count>10:
#                 break


#         warc_filenameList= [items['filename'] for items in index_results ]
#         for filename in warc_filenameList:
#             wet_data = pd.DataFrame({})

#             result = wet_extractor(filename)
#             if result is not None:
#                 wet_data = pd.DataFrame(result)
#                 print(wet_data)
#                 max_retries = 5
#                 retry_delay = 5
#                 for attempt in range(max_retries):
#                     try:
#                         es_host = 'http://3.108.185.168:9201'
#                         ep = es_pandas(es_host, timeout=30)
#                         index="commoncrawl_data"
#                         ep.to_es(wet_data, index, doc_type="_doc", show_progress=True, thread_count=2, chunk_size=10000)
#                     #    wet_data = pd.concat([wet_data,result], ignore_index=True)
#                     except ConnectionTimeout:
#                         if attempt < max_retries - 1:
#                             print(f"Connection timeout. Retrying in {retry_delay} seconds...")
#                             sleep(retry_delay)
#                         else:
#                             print("Max retries reached. Unable to establish connection.")

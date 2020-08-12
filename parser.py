import json
import requests
import psycopg2

con_file = open('config.json')
config = json.load(con_file)
con_file.close()

apiKey = config['apiKey']
searchURL = 'https://api.elsevier.com/content/search/scopus?'
fields = 'title,citedby-count,publicationName,creator,doi,affilname,affiliation-country,affiliation-city,affiliation-сountry,afid,authid,authname,authkeywords'

with open('files/source_id.csv', 'r') as f:
  F = [line.strip() for line in f]

length = len(F)

conn = psycopg2.connect(dbname=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASS'], host=config['DB_HOST'])


with conn:
    with conn.cursor() as cur:

        for sourceID, progress in zip(F, range(length)):
            query = 'SOURCE-ID(' + str(sourceID) + ') AND ( LIMIT-TO( DOCTYPE, "ar") OR LIMIT-TO( DOCTYPE, "cp" ) OR LIMIT-TO( DOCTYPE, "re") OR LIMIT-TO( DOCTYPE, "ch") )' # ISSN()

            url = searchURL+'start=0&count=1&apiKey='+apiKey+'&query='+query+'&field=publicationName&subjarea=ECON&date=2011-2020'

            response = requests.get(url)
            output = json.loads(response.text)
            totalResults = int(output['search-results']['opensearch:totalResults'])
            #itemsPerPage = int(output['search-results']['opensearch:itemsPerPage'])

            if totalResults != 0:
                for startPage in range(0, totalResults, 25):
                    url = searchURL+'start='+str(startPage)+'&count=25&apiKey='+apiKey+'&query='+query+'&subjarea=ECON&field='+fields+'&date=2011-2020&view=COMPLETE'
                    response = requests.get(url)

                    output = json.loads(response.text)
                    entries = output['search-results']['entry']

                    for entry in entries:
                      
                      
                      #cur.execute("INSERT INTO student (name) VALUES(%s)", ("David",))

            print(str(progress+1) + ' of ' + str(length) + ' done ('+ str(round((progress+1)/length, 2)) + '%)')


conn.commit()
conn.close()
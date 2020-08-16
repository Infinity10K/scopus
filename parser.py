# importing libraries

import json
import requests
import psycopg2
from re import sub

# functions (a separate module will be created in the future)


def get_type(table, data):
    sql_select = 'select * from ' + table + ' where type = %s;'
    sql_insert = 'insert into ' + table + ' (type) values(%s);'

    try:
        cur.execute(sql_select, (data, ))

        result = cur.fetchall()

        if not result:
            cur.execute(sql_insert, (data, ))

            cur.execute(sql_select, (data, ))

            result = cur.fetchall()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return result


def get_source(table, data):
    sql_select = 'select id from ' + table + ' where id = %s;'
    sql_insert = 'insert into ' + table + ' (id, issn, name, type_id) values(%s, %s, %s, %s);'

    try:
        cur.execute(sql_select, (data[0], ))

        result = cur.fetchall()

        if not result:
            cur.execute(sql_insert, data)

            cur.execute(sql_select, (data[0], ))

            result = cur.fetchall()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    return result

# opening config file

con_file = open('config.json')
config = json.load(con_file)
con_file.close()

# search params

apiKey = config['apiKey']
searchURL = 'https://api.elsevier.com/content/search/scopus?'
fields = 'title,citedby-count,publicationName,creator,doi,affilname,affiliation-country,affiliation-city,affiliation-сountry,afid,\
            authid,authname,authkeywords,source-id,coverDate,description,subtypeDescription,aggregationType,issn,identifier,'

# opening file with sources id

with open('files/source_id.csv', 'r') as f:
    F = [line.strip() for line in f]

length = len(F)

# db connection

conn = psycopg2.connect(dbname=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASS'], host=config['DB_HOST'])

# 

with conn:
    with conn.cursor() as cur:

        # for each source, 

        for sourceID, progress in zip(F, range(length)):

            # we search for the appropriate types of documents

            query = 'SOURCE-ID(' + str(sourceID) + ') AND ( LIMIT-TO( DOCTYPE, "ar") OR LIMIT-TO( DOCTYPE, "cp" ) OR LIMIT-TO( DOCTYPE, "re") OR LIMIT-TO( DOCTYPE, "ch") )'

            url = searchURL+'start=0&count=1&apiKey='+apiKey+'&query='+query+'&field=publicationName&subjarea=ECON&date=2011-2020'

            # getting the initial result

            response = requests.get(url)
            output = json.loads(response.text)
            totalResults = int(output['search-results']['opensearch:totalResults'])

            if totalResults != 0:  # if number of results not zero

                # we parse all pages

                for startPage in range(0, totalResults, 25):
                    url = searchURL+'start='+str(startPage)+'&count=25&apiKey='+apiKey+'&query='+query+'&subjarea=ECON&field='+fields+'&date=2011-2020&view=COMPLETE'
                    response = requests.get(url)

                    output = json.loads(response.text)
                    entries = output['search-results']['entry']

                    for entry in entries:

                        # add/get doc_type and source_type

                        doc_type_id = get_type('doc_types', entry['subtypeDescription'])[0][0]
                        source_type_id = get_type('source_types', entry['prism:aggregationType'])[0][0]

                        # add/get source
                        
                        data = (entry['source-id'], entry['prism:issn'], entry['prism:publicationName'], source_type_id)
                        source_id = get_source('sources', data)

                        # add/get authors

                        affil = []
                        sub_authors = []
                        creator = entry['dc:creator']

                        for affiliation in entry['affiliation']:
                            affil.append(entry['afid'])

                        article_id = sub(r'\D', '', entry['dc:identifier'])

                        for author in entry['author']:
                            if creator != author['authname']:
                                sub_authors.append(int(author['authid']))

                            auth = cur.execute('select id from authors where id = ' + author['authid'] + ';')

                            auth_affil = []

                            for affil_ in author['afid']:
                                auth_affil.append(int(affil_['$']))

                            if auth:
                                cur.execute('update authors set affilations = affilations || '+ affil_ +'::bigint[] WHERE id = ' + author['authid'])
                            else:
                                cur.execute('insert into authors (affilations, id, name) values(%s, %s, %s) on conflict do nothing;', 
                                (affil_, author['authid'], entry['authname']))

                            cur.execute('update authors set articles = articles || '+ article_id +'::bigint WHERE id = ' + author['authid'])

                        # add article in db

                        key_words = split(r'\W+', entry['authkeywords'])

                        cur.execute('insert into articles \
                            (affilations, authkeywords, citedby_count, creator, description, doctype, doi, id, pub_date, source_id, subauthors, title) \
                            values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing;', 
                            (affil, key_words, entry['citedby-count'], creator, entry['dc:description'], doc_type_id, entry['prism:doi'], 
                            article_id, entry['prism:coverDate'], source-id, sub_authors, entry['dc:title']))

                        # add affil in db

                        #

                        #cur.execute("INSERT INTO student (name) VALUES(%s)", ("David",))

            conn.commit()

            print(str(progress+1) + ' of ' + str(length) + ' done ('+ str(round((progress+1)/length, 2)) + '%)')


conn.close()

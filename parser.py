# importing libraries

import json
import requests
import psycopg2
from re import sub, split

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

        return result

    except (Exception, psycopg2.DatabaseError) as error:
        print(error, 'in function get_type')


def get_source(table, data):
    sql_select = 'select id from ' + table + ' where id = %s;'
    sql_insert = 'insert into ' + table + \
        ' (id, name, type_id) values(%s, %s, %s);'

    try:
        cur.execute(sql_select, (data[0], ))

        result = cur.fetchall()

        if not result:
            cur.execute(sql_insert, data)

            cur.execute(sql_select, (data[0], ))

            result = cur.fetchall()

        return result

    except (Exception, psycopg2.DatabaseError) as error:
        print(error, 'in function get_source')

# opening config file


con_file = open('config.json')
config = json.load(con_file)
con_file.close()

# search params

apiKey = config['apiKey']
searchURL = 'https://api.elsevier.com/content/search/scopus?'
fields = 'title,citedby-count,publicationName,creator,doi,affilname,affiliation-country,affiliation-city,affiliation-сountry,afid,authid,authname,authkeywords,source-id,coverDate,description,subtypeDescription,aggregationType,identifier'

# opening file with sources id

with open('files/source_id.csv', 'r') as f:
    F = [line.strip() for line in f]

length = len(F)
sum_results, sum_errors = 0, 0

# db connection

conn = psycopg2.connect(dbname=config['DB_NAME'], user=config['DB_USER'],
                        password=config['DB_PASS'], host=config['DB_HOST'])

#

with conn:
    with conn.cursor() as cur:

        # for each source,

        for sourceID, progress in zip(F, range(length)):

            # we search for the appropriate types of documents

            query = 'SOURCE-ID(' + str(sourceID) + \
                ') AND ( LIMIT-TO( DOCTYPE, "ar") OR LIMIT-TO( DOCTYPE, "cp" ) OR LIMIT-TO( DOCTYPE, "re") OR LIMIT-TO( DOCTYPE, "ch") )'

            url = searchURL+'start=0&count=1&apiKey='+apiKey+'&query=' + \
                query+'&subjarea=ECON&date=2011-2021'

            # getting the initial result

            response = requests.get(url)

            if response.status_code == 200:

                output = json.loads(response.text)
                totalResults = int(output['search-results']
                                   ['opensearch:totalResults'])

                if totalResults != 0:  # if number of results not zero

                    # we parse all pages

                    for startPage in range(0, totalResults, 25):
                        url = searchURL+'start='+str(startPage)+'&count=25&apiKey='+apiKey+'&query=' + \
                            query+'&subjarea=ECON&field='+fields+'&date=2011-2021&view=COMPLETE'

                        response = requests.get(url)

                        if response.status_code == 200:

                            output = json.loads(response.text)
                            entries = output['search-results']['entry']

                            for entry in entries:
                                try:
                                    try:
                                            subtypeDescription = entry['subtypeDescription']
                                            aggregationType = entry['prism:aggregationType']
                                            source_id = entry['source-id']
                                            publicationName = entry['prism:publicationName']
                                            identifier = entry['dc:identifier']
                                            affiliations = entry['affiliation']
                                            creator = entry['dc:creator']
                                    
                                    except:
                                            sum_errors += 1
                                            continue

                                        # add/get doc_type and source_type
                                    if subtypeDescription in 'Article, Conference Paper, Review, Book Chapter' and aggregationType in 'Journal, Conference Proceeding':

                                        doc_type_id = get_type(
                                            'doc_types', subtypeDescription)[0][0]
                                        source_type_id = get_type(
                                            'source_types', aggregationType)[0][0]

                                        # add/get source

                                        source_id = get_source(
                                            'sources', (source_id, publicationName, source_type_id))[0][0]

                                        #

                                        affil = []
                                        article_id = sub(r'\D', '', identifier)

                                        # add affil in db and get array of affil_id

                                        for affiliation in affiliations:

                                            afid = affiliation['afid']

                                            affil.append(int(afid))

                                            cur.execute('select id from affiliations where id = ' + afid + ';')

                                            res = cur.fetchall()

                                            if res:
                                                cur.execute(
                                                    'update affiliations set articles = articles || ' + article_id + '::bigint WHERE id = ' + afid + ';')
                                            else:
                                                cur.execute('insert into affiliations (articles, city, country, id, name) values(array[%s]::bigint[], %s, %s, %s, %s);',
                                                            (int(article_id), affiliation['affiliation-city'], affiliation['affiliation-country'], afid, affiliation['affilname']))

                                        # add/get authors

                                        sub_authors = []

                                        for author in entry['author']:

                                            authid = author['authid']

                                            authname = author['authname']

                                            if creator != authname:
                                                sub_authors.append(int(authid))
                                            else:
                                                creator_id = int(authid)

                                            auth_affil = []

                                            for affil_ in author['afid']:
                                                auth_affil.append(int(affil_['$']))

                                            cur.execute('select id from authors where id = ' + authid + ';')

                                            auth = cur.fetchall()

                                            if auth:
                                                cur.execute('update authors set affiliations = affiliations || %s::bigint[] WHERE id = ' + authid + ';', (auth_affil, ))
                                            else:
                                                cur.execute('insert into authors (affiliations, id, name) values(%s, %s, %s);',
                                                            (auth_affil, authid, authname))

                                            cur.execute('update authors set articles = articles || ' +
                                                        article_id + '::bigint WHERE id = ' + authid + ';')

                                        # add article in db

                                        try:
                                            key_words = sub(r'\W+', ' ', entry['authkeywords'])
                                        except:
                                            key_words = []

                                        try:
                                            description = entry['dc:description']
                                        except:
                                            description = ''

                                        cur.execute('insert into articles (affiliations, authkeywords, citedby_count, creator, description, doc_type, id, pub_date, source_id, subauthors, title) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing;',
                                                    (affil, key_words, entry['citedby-count'], creator_id, description, doc_type_id,  
                                                    article_id, entry['prism:coverDate'], source_id, sub_authors, entry['dc:title']))
                                        
                                        conn.commit()
                                except (Exception, psycopg2.DatabaseError) as error:
                                    sum_errors += 1
                                    print(error)
                                

                    

                print(str(progress+1) + ' of ' + str(length) +
                      ' done (' + str(round((progress + 1) * 100 / length, 2)) + '%)')

                sum_results += totalResults
                print('Results:', sum_results, '| Errors:', sum_errors)


conn.close()

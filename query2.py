from __future__ import print_function
import pyArango.connection
import pprint as pp
# import json


conn = pyArango.connection.Connection(arangoURL='http://192.168.1.45:8529')
print(conn.databases)
db = conn['_system']
print(db)

qry = "FOR keywordMatch IN 1..1 OUTBOUND 'JOT-campaigns-germany/546426629' GRAPH 'JOT-campaigns-germany' OPTIONS {bfs: true, uniqueVertices: 'global'} "
qry += "FILTER 'https://www.google.com/rdf#AdWordMatch' IN keywordMatch.type "
qry += "RETURN {cityName: keywordMatch.`jot:inCityName`, date: keywordMatch.`dbp:date`, matchKey: keywordMatch._key}"

print('')
queryResult = db.AQLQuery(qry, rawResults=True, batchSize=8)
print(queryResult)

# this loops over all results in batches of batchSize
# print('***')
# for qq in queryResult:
#     print(qq)
#     print('')

# only show the first few
print('***')
for ii, qq in enumerate(queryResult):
    print(qq)
    if ii == 8:
        break

# parse json and pretty print
# this isn't working the way I thought... :s
# print('***')
# pp.pprint(json.loads(qq))

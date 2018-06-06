from __future__ import print_function
import pyArango.connection
import pprint as pp
import json


conn = pyArango.connection.Connection(arangoURL='http://192.168.1.45:8529')
print(conn.databases)
db = conn['_system']
print(db)

campaign_id = 3317658057 # may be cached
# campaign_id = 546426629  # will never be cached (why???)
qry = "FOR keywordMatch IN 1..1 OUTBOUND 'JOT-campaigns-germany/%i' GRAPH 'JOT-campaigns-germany' OPTIONS {bfs: true, uniqueVertices: 'global'} " % campaign_id
qry += "FILTER 'https://www.google.com/rdf#AdWordMatch' IN keywordMatch.type "
qry += "RETURN {cityName: keywordMatch.`jot:inCityName`, date: keywordMatch.`dbp:date`, matchKey: keywordMatch._key}"
print(qry)

print('')
queryResult = db.AQLQuery(qry, rawResults=True, batchSize=64, count=True)
print(queryResult)
print('length:', len(queryResult))
print('count:', queryResult.count)

print('###')
pp.pprint(queryResult.response)
print('###')

if queryResult.response['cached'] == True:
    print('CACHED')

# this loops over all results in batches of batchSize
# print('***')
# for qq in queryResult:
#     print(qq)
#     print('')

# only show the first few
# print('***')
# for ii, qq in enumerate(queryResult):
#     print(json.dumps(qq))
#     if ii == 8:
#         break

# append all results
print('***')
res = []
for ii, qq in enumerate(queryResult):
    # print(qq)
    res.append(qq)
    if ii == 32:
        break

print('///')
# print(json.dumps(queryResult))
# pp.pprint(json.dumps(res))
pp.pprint(res[20:28])
print('A')
print('A')
print('A')
pp.pprint(json.dumps(res[20:28]))
print('V')
print('V')
print('V')
print(json.dumps(res[20:28]))

# parse json and pretty print
# this isn't working the way I thought... :s
# print('***')
# pp.pprint(json.loads(qq))

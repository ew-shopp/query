from __future__ import print_function
import pyArango.connection
import json
import time


def connect_to_database():
    conn = pyArango.connection.Connection(arangoURL='http://192.168.1.45:8529')
    db = conn['_system']
    return db


def read_query_from_file(fname):
    with open(fname, 'r') as f:
        lines = f.readlines()
    qry = ''.join(lines)
    return qry


# run query against database
# for Bind Parameters (aka bindVars), see
# https://github.com/tariqdaouda/pyArango#queries--aql
# https://docs.arangodb.com/3.3/AQL/Fundamentals/BindParameters.html
# do note the peculiarities about string processing
# also see query4.aql
def run_query(db, qry, campaign_key=546426629):
    queryResult = db.AQLQuery(qry, rawResults=True, batchSize=64, count=True, \
                              bindVars={'campaign_key': campaign_key})
    return queryResult


def query_to_result_list(queryResult):
    t0 = time.time()
    result_list = []
    for ii, qq in enumerate(queryResult):
        if ii % 5000 == 0:
            t1 = time.time()
            print("** Fetched %07d/%07d Records (%.1fs Elapsed)" % (ii, queryResult.count, t1-t0))
        result_list.append(qq)
    t1 = time.time()
    print("** Fetched %06d/%06d Records (%.1fs Elapsed)" % (ii+1, queryResult.count, t1-t0))
    return result_list


# write query output to file
def result_list_to_file(result_list, fname):
    with open(fname, 'w') as f:
        json.dump(result_list, f)


print('// Connecting to Database')
db = connect_to_database()

print('// Loading Query')
qry = read_query_from_file('query4.aql')
print('** Query:')
print(qry)

print('// Excecuting Query')
queryResult = run_query(db, qry)

print("** Query Returned %i Records" % queryResult.count)
if queryResult.response['cached'] == True:
    print("** Query Was Cached")
else:
    print("** Query Took %.2f Seconds" % queryResult.response['extra']['stats']['executionTime'])
print('// Fetching Resulting. Wallclock Ticking.')
result_list = query_to_result_list(queryResult)

fname = "test.json"
print("// Writing Result to File (%s)" % fname)
t0 = time.time()
result_list_to_file(result_list, fname)
t1 = time.time()
print("** Took %.1fs" % (t1-t0))

print('!! Done')

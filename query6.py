from __future__ import print_function
import pyArango.connection
import json
import time
import argparse


def connect_to_database():
    conn = pyArango.connection.Connection(arangoURL='http://192.168.1.45:8529')
    db = conn['_system']
    return db


def read_campaign_keys_from_json(fname):
    with open(fname, 'r') as f:
        json_of_keywords = json.load(f)
    campaign_keys = []
    for json_of_keyword in json_of_keywords:
        campaign_keys.append(int(json_of_keyword['key']))
    return campaign_keys


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
# also see query6.aql
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


print('// Parsing Arguments')
parser = argparse.ArgumentParser()
parser.add_argument('--keyfile', default='1m10small.json', \
                    help='Name of JSON File w/ Campaign Keys.')
args = parser.parse_args()

print('// Connecting to Database')
db = connect_to_database()

print("// Loading Campaign Keys (%s)" % args.keyfile)
campaign_keys = read_campaign_keys_from_json(args.keyfile)
print('** List of Requested Keys is:')
print(campaign_keys)

print('// Looping Over Campaign Keys')
print('')
for campaign_key in campaign_keys:
    t00 = time.time()

    print("**** Running Query/Fetch/Save for Campaign Key %i" % campaign_key)
    print(">>>> %s UTC" % time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))

    print('// Loading Query')
    qry = read_query_from_file('query6.aql')
    print('** Query:')
    print(qry)

    print("// Excecuting Query for Campaign Key %i" % campaign_key)
    queryResult = run_query(db, qry, campaign_key=campaign_key)

    print("** Query Returned %i Records" % queryResult.count)
    if queryResult.response['cached'] == True:
        print("** Query Was Cached")
    else:
        print("** Query Took %.2f Seconds" % queryResult.response['extra']['stats']['executionTime'])
    print('// Fetching Resulting. Wallclock Ticking.')
    result_list = query_to_result_list(queryResult)

    fname = "result_%i.json" % campaign_key
    print("// Writing Result to File (%s)" % fname)
    t0 = time.time()
    result_list_to_file(result_list, fname)
    t1 = time.time()
    print("** Took %.1fs" % (t1-t0))

    print("**** Finished for Campaign Key %i" % campaign_key)
    print("**** Round-Trip Time %.1fs" % (time.time() - t00))
    print(">>>> %s UTC" % time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    print('')

print('!! Done')

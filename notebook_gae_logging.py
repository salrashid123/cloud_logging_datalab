
#%%

print '1 ------------------------------------------'

import google.api.servicecontrol.v1.log_entry_pb2
import google.iam.v1.logging.audit_data_pb2
import google.appengine.v1.audit_data_pb2
import google.cloud.bigquery.logging.v1.audit_data_pb2
import google.cloud.audit.audit_log_pb2
import google.appengine.logging.v1.request_log_pb2


import os
import pprint
import pandas

from google.cloud import logging
import google.appengine.logging.v1.request_log_pb2

#os.environ["GOOGLE_CLOUD_DISABLE_GRPC"] = "true"
pp = pprint.PrettyPrinter(indent=4)

FILTER = ('resource.type="gae_app" AND logName="projects/mineral-minutia-820/logs/appengine.googleapis.com%2Frequest_log" AND timestamp >= "2018-08-30T00:00:00Z" AND resource.labels.module_id="default" AND resource.labels.version_id="1"')

client = logging.Client(project='mineral-minutia-820')

entries = []
for entry in client.list_entries(filter_=FILTER):
    entries.append(entry)

payloads = [e.payload for e in entries]

print 'done'
#%%

print '2 ------------------------------------------'


df = pandas.DataFrame(payloads)
df['endTime'] = pandas.to_datetime(df['endTime'])
df['startTime'] = pandas.to_datetime(df['startTime'])
df['latency'] = df['latency'].apply(lambda x: float(x[:-1]))
df['responseSize'] = df['responseSize'].apply(float)
df['megaCycles'] = df['megaCycles'].apply(float)
df['versionId'] = df['versionId']
df['num_lines'] = df['line'].apply(lambda x: len(x) if isinstance(x, list) else 0)
df['timestamp'] = pandas.to_datetime([e.timestamp for e in entries])
df = df.set_index('timestamp', drop=True).sort_index()

print 'done'

#%%
print '3 ------------------------------------------'

df.columns.tolist()

#%%
print '4 ------------------------------------------'

df['resource'].value_counts()

#%%
print '5 ------------------------------------------'
df.groupby('resource')['cost'].sum()


#%%
print '6 ------------------------------------------'
df.groupby('resource')['responseSize'].mean()


#%%
print '7 ------------------------------------------'
df['latency'].plot()


#%%
print '8 ------------------------------------------'
import collections
import json

def flatten(d, parent_key='', sep=''):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_lines(lines):
    if not isinstance(lines, list):
        return
    for l in lines:
        print l
        try:
            log_message = json.loads(l.get('logMessage', ''))
            print 'json'
        except ValueError:
            print 'not json'
            continue

        if not isinstance(log_message, dict):
            continue
        flattened_data = flatten({'logMessage': log_message})
        l.update(flattened_data)


#%%
print '9 ------------------------------------------'

selected_lines = df['line'][1]
flatten_lines(selected_lines)
df_line = pandas.DataFrame(selected_lines)
df_line['time'] = pandas.to_datetime(df_line['time'])
df_line = df_line.set_index('time', drop=True)
if "RequestId:" in df_line['logMessage']:
    df_line['logMessage_trace'] = df_line['logMessage'].split(" ")[1].fillna(0.0)
#df_line['logMessage_fruits'] = df_line['logMessage'].fillna('')
df_line


#%%
print '10 ------------------------------------------'

df_all_lines = pandas.DataFrame()

for row in df.itertuples(index=True):
    lines = row.line
    if not isinstance(lines, list):
        continue
    flatten_lines(lines)
    df_tmp = pandas.DataFrame(lines)
    df_tmp['entry_timestamp'] = row.Index
    df_tmp['time'] = pandas.to_datetime(df_tmp['time'])
    df_tmp = df_tmp.set_index(['entry_timestamp', 'time'], drop=True)
    df_all_lines = df_all_lines.append(df_tmp)

df_all_lines = df_all_lines.sort_index()
df_all_lines

#%%
print '11 ------------------------------------------'

df_all_lines['logMessage_fruits'].value_counts()

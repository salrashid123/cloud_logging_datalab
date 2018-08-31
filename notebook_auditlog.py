


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
import copy
import collections
import pandas
from google.cloud import logging

#os.environ["GOOGLE_CLOUD_DISABLE_GRPC"] = "true"
pp = pprint.PrettyPrinter(indent=4)

FILTER = 'resource.type="bigquery_resource"  AND timestamp >= "2018-08-28T01:48:49.275Z"'

client = logging.Client(project='mineral-minutia-820')

entries = []
for entry in client.list_entries(filter_=FILTER):
    entries.append(entry)


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


payloads = [copy.deepcopy(e.payload) for e in entries]

for p in payloads:
    service_data = p.get('serviceData', {})
    if not isinstance(service_data, dict):
        service_data = {}
    flattened_data = flatten({'serviceData': service_data})
    p.update(flattened_data)
    p.pop('serviceData', None)
    

df = pandas.DataFrame(payloads)

print 'Done Loading Dataframe'


#%%
print ' ############ Listing Columns in datafarame: '

print df.columns.tolist()


#%%
print ' ############ Listing jobName_jobId: '

print df[u'serviceData_jobCompletedEvent_job_jobName_jobId'].dropna() 


#%%
print ' ############ Listing jobStatistics_totalProcessedBytes: '

print  df[u'serviceData_jobCompletedEvent_job_jobStatistics_totalProcessedBytes'].value_counts()

#%%
print '9 ------------------------------------------'

print df['methodName'].value_counts()

print 'done'

#%%
print '10 ------------------------------------------'


print df.groupby('methodName')['serviceData_jobCompletedEvent_job_jobStatistics_totalProcessedBytes'].max()

print 'done'
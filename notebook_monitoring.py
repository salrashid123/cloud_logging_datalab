

#%%

import google.datalab
import google.datalab.stackdriver.monitoring as gcm

import collections
import StringIO

import pandas 
import os
#os.environ["GOOGLE_CLOUD_DISABLE_GRPC"] = "true"

query_cpu = gcm.Query('compute.googleapis.com/instance/cpu/utilization', hours=7*24)
cpu_metadata = query_cpu.metadata()

instance_prefix_counts = collections.Counter(
  timeseries.metric.labels['instance_name'].rsplit('-', 1)[0]
  for timeseries in cpu_metadata)
instance_prefix_counts.most_common(5)

common_prefix = "ad-dc1"
print 'done'

#%%
query_cpu = query_cpu.select_metrics(instance_name_prefix=common_prefix)

  # Aggregate to hourly intervals per zone.
query_cpu = query_cpu.align(gcm.Aligner.ALIGN_MEAN, hours=1)
query_cpu = query_cpu.reduce(gcm.Reducer.REDUCE_MEAN, 'resource.zone')

# Get the time series data as a dataframe, with a single-level header.
per_zone_cpu_data = query_cpu.as_dataframe(label='zone')
per_zone_cpu_data.tail(5)


#%%

# Extract the number of days in the dataframe.
num_days = len(per_zone_cpu_data.index)/24

# Split the big dataframe into daily dataframes.
daily_dataframes = [per_zone_cpu_data.iloc[24*i: 24*(i+1)]
                    for i in xrange(num_days)]

# Reverse the list to have today's data in the first index.
daily_dataframes.reverse()

# Display the last five rows from today's data.
daily_dataframes[0].tail(5)


#%%

TODAY = 'Today'

# Helper function to make a readable day name based on offset from today.
def make_day_name(offset):
  if offset == 0:
    return TODAY
  elif offset == 1:
    return 'Yesterday'
  return '%d days ago' % (offset,)


#%%

# Extract the zone names.
all_zones = per_zone_cpu_data.columns.tolist()

# Use the last day's timestamps as the index, and initialize a dataframe per zone.
last_day_index = daily_dataframes[0].index
zone_to_shifted_df = {zone: pandas.DataFrame([], index=last_day_index)
                      for zone in all_zones}

for i, dataframe in enumerate(daily_dataframes):
  # Shift the dataframe to line up with the start of the last day.
  dataframe = dataframe.tshift(freq=last_day_index[0] - dataframe.index[0])
  current_day_name = make_day_name(i)
  
  # Insert each daily dataframe as a column into the dataframe.
  for zone in all_zones:
    zone_to_shifted_df[zone][current_day_name] = dataframe[zone]
    
# Display the first five rows from the first zone.    
zone_to_shifted_df[all_zones[0]].head(5)


#%%

for zone, dataframe in zone_to_shifted_df.iteritems():
  dataframe.plot(title=str(zone)).legend(loc="upper left", bbox_to_anchor=(1,1))

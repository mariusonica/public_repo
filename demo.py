import pandas as pd

# Read the CSV file
data = pd.read_csv(r"C:\Users\marius.onica\Downloads\bq-results-20231220-134345-1703079855754.csv")

# Filter data for engagements in 2023 and split the topics
data = data[data['date'].str.contains('2023')]
split_data = data[data['date'].str.contains('2023')]

# Split values in 'topic' and explode into rows
split_data['topic'] = data['topic'].str.split(';')
split_data = data.explode('topic').reset_index(drop=True)

# Group data by user, topic, and engagement type and Group splited data by user, topic, and engagement type
grouped_data = data.groupby(['scv_id', 'topic', 'source_system']).size().reset_index(name='count')
grouped_split_data = split_data.groupby(['scv_id', 'topic', 'source_system']).size().reset_index(name='count')

# Calculate the maximum count of engagements for each user/topic combination
max_counts = grouped_data.groupby(['scv_id', 'topic'])['count'].max().reset_index(name='max_count')
max_counts_split = grouped_split_data.groupby(['scv_id', 'topic'])['count'].max().reset_index(name='max_count')

# Merge the maximum counts with the grouped data
merged_data = pd.merge(grouped_data, max_counts, on=['scv_id', 'topic'])
merged_data_split = pd.merge(grouped_split_data, max_counts_split, on=['scv_id', 'topic'])

# Calculate the affinity score for each user/topic combination
merged_data['affinity_score'] = merged_data['count'] / merged_data['max_count'] * 10
merged_data_split['affinity_score'] = merged_data_split['count'] / merged_data_split['max_count'] * 10

# Generate a new CSV file with the user, topic, and affinity score
result_aff_score = merged_data[['scv_id', 'topic', 'source_system', 'affinity_score']]
result_topic = merged_data[['scv_id', 'topic', 'source_system', 'count', 'max_count', 'affinity_score']]
result_split_topic = merged_data_split['affinity_score'] = merged_data_split['count'] / merged_data_split['max_count'] * 10
[['scv_id', 'topic', 'source_system', 'count', 'max_count', 'affinity_score']]

#Save results in csv files
result_aff_score.to_csv('affinity_scores.csv', index=False)
result_topic.to_csv('topic_affinity_scores.csv', index=False)
result_split_topic.to_csv('split_topic_affinity_scores.csv', index=False)

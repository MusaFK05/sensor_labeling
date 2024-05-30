from .processor import SensorDataProcessor

# class sensor:
#     def __init__(self, df):
#         self.df = df

#     def labeling(self):
#         process_data = SensorDataProcessor(self.df)
#         process_data.apply_mappings()
#         processed_df = process_data.process_data()
#         return processed_df

def labeling(df):
    process_data = SensorDataProcessor(df)
    process_data.apply_mappings()
    processed_df = process_data.process_data()
    return processed_df

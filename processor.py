import pandas as pd

class SensorDataProcessor:
    def __init__(self, df):
        self.df = df

    def sensor_col(self, device, sensor):
        self.df.loc[self.df['device_id'] == device, 'sensor'] = sensor

    def apply_mappings(self):
        # Applying individual mappings
        mappings = [
            {'index_id': '638d8677245a8f9cc00b4bcc', 'device_id': 'AMJ6RHSdwf4XMv2dhPRh8hxnE92exg7U', 'sensor': 'temp'},
            {'index_id': '6142a70046514f50ff8ed6a7', 'device_id': 'AMJ6RHSdwf4XMv2dhPRh8hxnE92exg7U', 'sensor': 'humi'},
            {'index_id': '6280a8465a0c89673d266101', 'device_id': 'GDdR9vUe3yXQWcfhP6grCLK74ZV4QZFL', 'sensor': 'N1'},
            {'index_id': '6280a8465a0c89673d266101', 'device_id': 'gxaZkwZafNVweTq8HycMKpZMz9MvbTyh', 'sensor': 'N2'},
            {'index_id': '6280a8275a0c89673d266100', 'device_id': 'GDdR9vUe3yXQWcfhP6grCLK74ZV4QZFL', 'sensor': 'P1'},
            {'index_id': '6280a8275a0c89673d266100', 'device_id': 'gxaZkwZafNVweTq8HycMKpZMz9MvbTyh', 'sensor': 'P2'},
            {'index_id': '6280a8505a0c89673d266102', 'device_id': 'GDdR9vUe3yXQWcfhP6grCLK74ZV4QZFL', 'sensor': 'K1'},
            {'index_id': '6280a8505a0c89673d266102', 'device_id': 'gxaZkwZafNVweTq8HycMKpZMz9MvbTyh', 'sensor': 'K2'},
            {'index_id': '611f7d7f4750382956b468e4', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R1'},
            {'index_id': '61305378590b802f53935e9a', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R2'},
            {'index_id': '6130523e590b802f53935e99', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R3'},
            {'index_id': '618f89476941b53a5d35606f', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R4'},
            {'index_id': '61910bcfd2cd6b06225ee0ca', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R5'},
            {'index_id': '618dc5c2553f46dc235bcfed', 'device_id': 'ODw83libBAixNsPMGTmqQer2gn2mZrOC', 'sensor': 'C1R6'},
            {'index_id': '611f7d7f4750382956b468e4', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R1'},
            {'index_id': '61305378590b802f53935e9a', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R2'},
            {'index_id': '6130523e590b802f53935e99', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R3'},
            {'index_id': '618f89476941b53a5d35606f', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R4'},
            {'index_id': '61910bcfd2cd6b06225ee0ca', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R5'},
            {'index_id': '618dc5c2553f46dc235bcfed', 'device_id': 'XniD6mBlnKqagRJ8qD9WhR6JGK4yle1d', 'sensor': 'C2R6'},
            {'index_id': '611f7d7f4750382956b468e4', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R1'},
            {'index_id': '61305378590b802f53935e9a', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R2'},
            {'index_id': '6130523e590b802f53935e99', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R3'},
            {'index_id': '618f89476941b53a5d35606f', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R4'},
            {'index_id': '61910bcfd2cd6b06225ee0ca', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R5'},
            {'index_id': '618dc5c2553f46dc235bcfed', 'device_id': 'Tdr4a4bKp5AzrCe6KGki8bUDF0ynE9l9', 'sensor': 'C3R6'}
        ]

        for mapping in mappings:
            self.df.loc[
                (self.df['index_id'] == mapping['index_id']) & 
                (self.df['device_id'] == mapping['device_id']), 
                'sensor'
            ] = mapping['sensor']

        # Applying bulk mappings
        sensor_mappings = [
            ('fPMkkgECQndBCs7eFtha09uy57Qv8Xks', 'EC1'),
            ('wqZeXsBhFSL6CLzfaUjJsnavudV3WvL7', 'EC2'),
            ('BngyuCFVukyQakpJyBug4WubAdpnt2g5', 'ST1'),
            ('J3c6xgg64gyL8pJ5uCZw69Ec4FJBj97R', 'ST2'),
            ('D8fRCvhyRWUNtzfWuhbdb9q5azNkrE4g', 'PH1'),
            ('lWwWZ7RHI5HToRocg122mLHgmqKsT7F7', 'PH2'),
            ('GDdR9vUe3yXQWcfhP6grCLK74ZV4QZFL', 'NPK1'),
            ('gxaZkwZafNVweTq8HycMKpZMz9MvbTyh', 'NPK2')
        ]
        
        for device, sensor in sensor_mappings:
            self.sensor_col(device, sensor)

    def process_data(self):
        # Drop rows where 'sensor' is 'nan'
        data = self.df[['sensor', 'value', 'createdAt']].copy()
        data = data[data['sensor'] != 'nan']

        # Pivot table
        data = data.pivot_table(index='createdAt', columns='sensor', values='value', aggfunc='first')
        data = data.reset_index().rename_axis(columns={'sensor': None})
        data = data.set_index('createdAt')

        # Adjusting time and resampling
        data.index = data.index + pd.Timedelta(hours=7)
        data = data.resample('5T').mean()
        
        return data

def labeling(df):
    process_data = SensorDataProcessor(df)
    process_data.apply_mappings()
    processed_df = process_data.process_data()
    return processed_df

import pandas as pd
import datetime

# Settings
FILE_PATH = './Subchronic_complete.csv'
# FILE_PATH = './test_data_1.csv'

INTERMEDIATE_FILE_PATH = '20210502_211720_intermediate_table.csv'

COLUMNS_TO_DROP = [
    'chemical_casrn', 'guideline_name', 'species', 'dose', 'toxrefdb_tg_dose_unit',
    'duration', 'duration_unit', 'ldt', 'hdt', 'dose_unit', 'effect_category', 'loael_dose'
]


class DataAnalyzer:
    multiple_value_counter = 0

    def prep_data(self):
        original_file = pd.read_csv(FILE_PATH)
        minimized_file = original_file.drop(COLUMNS_TO_DROP, axis=1)
        print(len(minimized_file))
        chemicals = set(minimized_file['chemical_name'])
        endpoints = set(minimized_file['Endpoint'])

        chemicals = sorted(list(chemicals))
        endpoints = sorted(list(endpoints))

        data_for_new_file = []

        # Add "Increase/Decrease" values
        for chemical in chemicals:
            print(chemical)
            chemical_data = []
            for endpoint in endpoints:
                value = self.get_value(chemical,endpoint, minimized_file)
                chemical_data.append(value)
            data_for_new_file.append(chemical_data)
        new_data_frame = pd.DataFrame(data_for_new_file, index=chemicals, columns=endpoints)

        # Check and add noael values
        noael_dose_values = []
        noael_category_values = []
        for chemical in chemicals:
            result = original_file.query('chemical_name == @chemical')
            original_noael_doses = set(result['noael_dose'])
            original_noael_categories = set(result['Category'])
            # check if chemicals have consistent noael values and categories
            if len(original_noael_doses) == 1 and len(original_noael_categories) == 1:
                noael_dose_values.append(list(original_noael_doses)[0])
                noael_category_values.append(list(original_noael_categories)[0])
            else:
                noael_dose_values.append(None)
                noael_category_values.append(None)

        new_data_frame = new_data_frame.assign(noael_dose = noael_dose_values)
        new_data_frame = new_data_frame.assign(Category = noael_category_values)

        datetime_string = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = datetime_string + '_' + str('intermediate_table') + '.csv'
        new_data_frame.to_csv(out_file, sep=',', encoding='utf-8')

        return new_data_frame

    def get_value(self, chemical, endpoint, original_file):
        value = None
        result = original_file.query('chemical_name == @chemical & Endpoint == @endpoint')
        if len(result) == 0:
            pass
        elif len(result) > 1:
            self.multiple_value_counter += 1
        else:
            value = result.iloc[0]["direction"]
        return value

    def analyze_data(self, data_frame):
        print('Total count: {count}'.format(count=len(data_frame.index)))

        noael_categories = set(data_frame.Category)
        datetime_string = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        for category in noael_categories:
            if not category:
                continue
            category_data = data_frame.query('Category == @category')
            category_results = self.analyze_category_data(category_data)
            print('{category} count: {count}'.format(category=category, count=len(category_data)))

            out_file = datetime_string + '_' + str(category) + '.csv'
            category_results.to_csv(out_file, sep=',', encoding='utf-8', index=False)

    def analyze_category_data(self, data):
        results = []  # count, total valid values, endpoint, direction
        for endpoint in data.columns[:-2]:
            value_count = data[endpoint].value_counts()
            decrease_label = 'Decrease'
            increase_label = 'Increase'

            if decrease_label in value_count.index:
                decrease_count = value_count.loc[decrease_label]
            else:
                decrease_count = 0

            if increase_label in value_count.index:
                increase_count = value_count.loc[increase_label]
            else:
                increase_count = 0

            total_valid = increase_count + decrease_count
            results.append((increase_count, total_valid, endpoint, increase_label))
            results.append((decrease_count, total_valid, endpoint, decrease_label))

        results.sort(reverse=True)
        result_data_frame = pd.DataFrame(results, columns=['count', 'total_values_for_endpoint', 'endpoint', 'direction'])
        return result_data_frame


    def get_value_count_for_endpoint(self, value, endpoint, data_frame):
        value_count = data_frame[endpoint].value_counts()
        if value in value_count.index:
            count = value_count.loc[value]
        else:
            count = 0
        return count

    def print_statistics(self, data_frame):
        print('**********')
        print('Statistics:')
        print('Number of chemicals: {}'.format(len(data_frame)))
        print('Number of endpoints: {}'.format(len(data_frame.columns)-2))
        print('Number of chemicals with multiple noael values: {}'
              ''.format(data_frame['noael_dose'].isna().sum()))
        print('Number of multiple values for one chemical and endpoint: {}'
              ''.format(self.multiple_value_counter))


if __name__ == "__main__":
    analyzer = DataAnalyzer()

    # calculate intermediate table
    prepped_data_frame = analyzer.prep_data()

    #load existing intermediate table
    # prepped_data_frame = pd.read_csv(INTERMEDIATE_FILE_PATH)

    # calculate results and write csvs per category
    analyzer.analyze_data(prepped_data_frame)

    # print statistics
    analyzer.print_statistics(prepped_data_frame)
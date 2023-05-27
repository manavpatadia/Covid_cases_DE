# ~/Desktop/spark/./bin/spark-submit /home/manav/Documents/DE_Project/Project/data_etl.py

from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, when, substring, isnull, mean, to_date, date_format, last
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import pickle
import warnings
# Ignore all warnings
warnings.filterwarnings("ignore")
import glob
import copy
import tabula
from google.cloud import bigquery
from pprint import pprint
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
pd.options.display.float_format = '{:.2f}'.format

# Initialize SparkSession
spark = SparkSession.builder.appName("JSON to DataFrame").enableHiveSupport().getOrCreate()

script_path = "/home/manav/Documents/DE_Project/Project/"
input_path = script_path + "input/"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= input_path + "bigqueryproject-347515-787a798db183.json"

def extract_dataset_1_cases():
    
    df = spark.read.json(input_path+"mohfw.json")
    column_name = "value"
    column_values = df.select(column_name).rdd.flatMap(lambda x: x).collect()
    
    final_list = []
    c = 0
    for cv in column_values:
        if(c == 0):
            c = 1
            continue
        elif(cv != None):
            final_list.append([cv['report_time'], cv['state'], cv['confirmed_india'], cv['confirmed_foreign'], cv['cured'], cv['death'], cv['confirmed']])
    
    
    df_cases = spark.createDataFrame(final_list, ["report_time", "state", "confirmed_india", "confirmed_foreign", "cured", "death", "confirmed"])
    return df_cases



def extract_dataset_2_state_codes():
    df_states = spark.read.csv(input_path+"state_codes.csv", header=True, inferSchema=True)
    return df_states



def transform_dataset_1_2(df_cases, df_states):
    
    df_cases = df_cases.withColumn("state", when(col("state") == 'dn_dd', 'dh').otherwise(col("state")))
    
    def state_code_transform(state):
        return state.lower()[3:]
    
    
    
    def map_value_to_dictionary(name):
        return dictionary_states.get(name)
    
    
    
    state_name_udf = udf(state_code_transform)
    
    df_states = df_states.withColumn("code", state_name_udf(df_states["3166-2 code"]))
    
    dictionary_states = {row['code']: row['Subdivision name'] for row in df_states.select('Subdivision name', 'code').collect()}
    
    map_value_udf = udf(map_value_to_dictionary)
    
    df_cases = df_cases.withColumn("state_name", map_value_udf(col("state")))
    
    final_list_df = df_cases.withColumn('report_time', substring(col('report_time'), 0, 10))
    
    return final_list_df



df_cases = extract_dataset_1_cases()
df_states = extract_dataset_2_state_codes()

df_cases = transform_dataset_1_2(df_cases, df_states)

df_cases.show(10)
df_cases.count()
df_cases.describe()

def extract_dataset_3_vaccination():
    list_pdf = []
    pdf_path = input_path + "vaccination/" + "*.pdf"
    for path in glob.glob(pdf_path, recursive=True):
        list_pdf.append(path)
    list_pdf = sorted(list_pdf)
    print(len(list_pdf))
    return list_pdf


list_pdf = extract_dataset_3_vaccination()
#import subprocess
#command = ['pip', 'install', 'seaborn']
#subprocess.check_call(command)

all_tables_list = []
file_name_date_list = []
def transform_dataset_3_vaccination_1():
    # pdf to tables
    c = 1
    for f in list_pdf:
        try:
            file_name_date = f.split("/")[-1][:10]
            file_name_date_list.append(file_name_date)
            print(c, end = "  ")
            # Extract tables from the PDF
            tables2 = tabula.io.read_pdf(f, pages='all', multiple_tables=True, silent = True)
            all_tables_list.append(tables2)
            c = c + 1
        except Exception as e:
            pass
    with open(script_path + 'output/list_vaccination_pdf_tables.pickle', 'wb') as f:
        pickle.dump(all_tables_list, f)


load_vccination_table = False

if(load_vccination_table):
    transform_dataset_3_vaccination_1()
else:
    for f in list_pdf:
        file_name_date = f.split("/")[-1][:10]
        file_name_date_list.append(file_name_date)
    
    
    with open(script_path + 'output/list_vaccination_pdf_tables.pickle', 'rb') as f:
        all_tables_list = pickle.load(f)



def extract_from_2lines(x):
    y = x.split("\r(")
    if(len(y) > 1):
        y[1] = y[1].split(" ")[0]
        return y
    else:
        y.append(None)
        return y


def extract_from_2lines_new(x):
    y = x.split(" ")
    return y[0][1:]


def remove_commas(value):
    if(value == None or value == "" or str(value) == "nan" or value == np.NaN):
        value = 0
    return int(str(value).replace(',','').replace(' ',''))


def transform_dataset_3_vaccination_2():
    # transforming semi structured data to structured data
    c = 0
    transformed_tables_list = []
    for t in all_tables_list:
        temp_tables = copy.deepcopy(t)
        #print("Extracting info from: ", file_name_date_list[c], c)
        if(len(temp_tables) == 1):
            temp_tables = [None, None]
            t = pd.DataFrame(t[0])
            temp_tables[0] = t[:3].reset_index(drop=True)
            temp_tables[1] = t[5:].reset_index(drop=True)
        if(c == 12):
            df_table = copy.deepcopy(temp_tables[2])
        else:
            df_table = copy.deepcopy(temp_tables[1])
        if(c in [3, 204]):
            df_table = df_table.iloc[3:]
        elif(c in [255, 312, 313, 314, 315, 317]):
            df_table = df_table.iloc[2:]
        elif(c in [316, 318, 319]):
            df_table = df_table.iloc[4:]
        elif(c >= 320 and c <= 327):
            df_table = df_table.iloc[3:]
        elif(c == 389 or c in [12]):
            last_nan_index = df_table[df_table.iloc[:, 2] == 'A & N Islands'].last_valid_index()
            df_table = df_table.iloc[last_nan_index:]
        elif(c >= 328):
            last_nan_index = df_table[df_table.iloc[:, 1] == 'A & N Islands'].last_valid_index()
            df_table = df_table.iloc[last_nan_index:]
        else:
            df_table = df_table.iloc[1:]
        df_table = df_table.dropna(axis=1, how='all')
        df_table = df_table.reset_index(drop=True)
        if(c == 0):
            df_table.columns = ['s_num', 'State_UT', 'Session planned',	'Session completed', 'dose_1', 'dose_2', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', 'total_doses']]
        elif(c == 164):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table['total_doses'] = df_table.apply(lambda row: remove_commas(row['dose_1']) + remove_commas(row['dose_2']), axis=1)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', 'total_doses']]
        elif(c == 196):
            df_table.columns = ['State_UT', 'dose_1', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table['State_UT'] = df_table['State_UT'].str.replace(r'\d+', '', regex=True)
            df_table[['dose_1', 'dose_2']]= df_table['dose_1'].str.split(" ", expand=True)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', 'total_doses']]
        elif(c == 316):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_2', 'total_doses']]= df_table['dose_2'].str.split(" ", expand=True)
            df_table['total_doses2'] = df_table.apply(lambda row: remove_commas(row['total_doses']) + remove_commas(row['15_18_years_dose_1']), axis=1)
            df_table['total_doses'] = df_table['total_doses2']
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'total_doses']]
        elif(c <= 310):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', 'total_doses']]
        elif(c <= 317):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', 'total_doses', '15_18_years_dose_1']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table['total_doses2'] = df_table.apply(lambda row: remove_commas(row['total_doses']) + remove_commas(row['15_18_years_dose_1']), axis=1)
            df_table['total_doses'] = df_table['total_doses2']
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'total_doses']]
        elif(c == 339):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', '15_18_years_dose_1', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_1', 'dose_2']]= df_table['dose_1'].str.split(" ", expand=True)
            df_table[['15_18_years_dose_1', '15_18_years_dose_2']]= df_table['15_18_years_dose_1'].str.split(" ", expand=True)
            df_table.loc[2, ['dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose', 'total_doses']] = \
                        df_table.loc[3, ['dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose',     'total_doses']]
            df_table['State_UT'] = df_table['State_UT'].replace('Arunachal', 'Arunachal Pradesh')
            df_table = df_table.drop([3, 4])
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose', 'total_doses']]
        elif((c>=318 and c<=330) or c in [332, 333, 337]):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_1', 'dose_2', '15_18_years_dose_1']]= df_table['dose_1'].str.split(" ", expand=True)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose', 'total_doses']]
        elif(c in [331, 334, 335, 336, 338]):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', 'precaution_dose', 'total_doses']]
        elif(c in [340, 341, 344, 345, 353, 355, 361, 363, 365, 366, 371, 374, 380, 382]):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', 'precaution_dose', 'total_doses']]
        elif(c in [342, 343, 346, 347, 348, 349, 350, 351, 352, 354, 356, 
                   357, 358, 359, 360, 362, 364, 367, 368, 369, 370, 372, 
                   373, 375, 376, 377, 378, 379, 381, 383]):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2']]= (df_table['dose_1']+" "+df_table['dose_2']).str.split(" ", expand=True)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', 'precaution_dose', 'total_doses']]
        elif(c == 389):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', 'precaution_dose', 'total_doses']]
        elif(c >= 409 and c <=411):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1']]= (df_table['dose_1']+" "+df_table['dose_2']).str.split(" ", expand=True)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', 'precaution_dose', 'total_doses']]
        elif(c >= 384 and c <= 408):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '12_14_years_dose_1', 'precaution_dose', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table[['dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2']]= (df_table['dose_1']+" "+df_table['dose_2']).str.split(" ", expand=True)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', 'precaution_dose', 'total_doses']]
        elif(c in [527,528,532,538,570,573,578,579,583] or (c >= 546 and c <= 556)):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', '12_14_years_dose_1', 'precaution_dose_18_59', 'total_doses']
            df_table['date_recorded'] = file_name_date_list[c]
            df_table.dropna(how='any', inplace=True)
            #df_table = df_table[df_table.iloc[:, 1] != 'Miscellaneous']
            df_table[['dose_1', 'dose_2']]= (df_table['dose_1']).str.split(" ", expand=True)
            df_table[['15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2', 'precaution_dose_18_59', 'precaution_dose_60_plus']] = \
            (df_table['12_14_years_dose_1']+" "+df_table['precaution_dose_18_59']).str.split(" ", expand=True)
            df_table['precaution_dose'] = df_table.apply(lambda row: remove_commas(row['precaution_dose_18_59']) + remove_commas(row['precaution_dose_60_plus']), axis=1)
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                 'precaution_dose', 'total_doses']]
        elif(c in [529, 530, 531, 533, 534, 535, 536, 537] or (c >= 539 and c<= 545) or (c >= 557 and c<=569) or (c>=571)):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                'precaution_dose_18_59', 'precaution_dose_60_plus', 'total_doses', 'nan1', 'nan2']
            df_table.drop(['nan1', 'nan2'], axis=1, inplace=True)
            df_table.dropna(axis=0, inplace=True)
            df_table['precaution_dose'] = df_table.apply(lambda row: remove_commas(row['precaution_dose_18_59']) + remove_commas(row['precaution_dose_60_plus']), axis=1)
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                 'precaution_dose', 'total_doses']]
        elif(c in [439, 522]):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', '15_18_years_dose_1', 'precaution_dose_18_59', 'total_doses']
            df_table[['dose_1', 'dose_2']]= (df_table['dose_1']).str.split(" ", expand=True)    
            df_table[['15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2']]= (df_table['15_18_years_dose_1']).str.split(" ", expand=True)
            df_table[['precaution_dose_18_59', 'precaution_dose_60_plus']]= (df_table['precaution_dose_18_59']).str.split(" ", expand=True)
            df_table['precaution_dose'] = df_table.apply(lambda row: remove_commas(row['precaution_dose_18_59']) + remove_commas(row['precaution_dose_60_plus']), axis=1)
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                 'precaution_dose', 'total_doses']]
        elif(c >= 412):
            df_table.columns = ['s_num', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                'precaution_dose_18_59', 'precaution_dose_60_plus', 'total_doses']
            df_table['precaution_dose'] = df_table.apply(lambda row: remove_commas(row['precaution_dose_18_59']) + remove_commas(row['precaution_dose_60_plus']), axis=1)
            df_table['date_recorded'] = file_name_date_list[c]
            df_table = df_table[['date_recorded', 'State_UT', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2',
                                 'precaution_dose', 'total_doses']]        
        transformed_tables_list.append(df_table)
        c = c + 1
    return transformed_tables_list



transformed_tables_list = transform_dataset_3_vaccination_2()


def transform_dataset_3_vaccination_3():
    #merge and clean all tables
    transformed_tables_list_2 = copy.deepcopy(transformed_tables_list)
    
    merged_df = pd.concat(transformed_tables_list_2, axis=0)
    merged_df = merged_df.reset_index(drop=True)
    
    mask = merged_df.apply(lambda row: '1st Dose' in row.values, axis=1)
    merged_df = merged_df[~mask]
    
    columns_to_apply = ['dose_1','dose_2','total_doses','precaution_dose','15_18_years_dose_1','15_18_years_dose_2','12_14_years_dose_1','12_14_years_dose_2']
    for col in columns_to_apply:
        merged_df[col] = merged_df[col].apply(remove_commas)
    
    merged_df.State_UT.unique()
    
    merged_df['State_UT'] = merged_df['State_UT'].replace('Andaman and Nicobar\rIslands', 'Andaman and Nicobar Islands')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Dadra & Nagar\rHaveli', 'Dadra & Nagar Haveli')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Arunachal\rPradesh', 'Arunachal Pradesh')
    
    merged_df['State_UT'] = merged_df['State_UT'].replace('A & N Islands', 'Andaman and Nicobar Islands')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Punjab*', 'Punjab')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Chhattisgarh*', 'Chhattisgarh')
    
    merged_df = merged_df[~(merged_df['State_UT'] == 'Haveli')]
    
    merged_df.loc[merged_df['dose_1'] == 5250505, 'State_UT'] = 'Himachal Pradesh'
    merged_df.loc[merged_df['dose_1'] == 5789969, 'State_UT'] = 'Jammu & Kashmir'
    
    condition1 = merged_df['date_recorded'] == '2021-04-02'
    condition2 = merged_df['dose_1'] == 0
    mask = condition1 & condition2
    merged_df = merged_df[~mask]
    
    merged_df[merged_df['State_UT'].isnull()]
    
    condition3 = merged_df['State_UT'].isnull()
    mask = condition3
    merged_df.loc[condition3, 'State_UT'] = 'Dadra and Nagar Haveli'
    
    condition1 = merged_df['State_UT'] == 'Dadra & Nagar'
    condition2 = merged_df['dose_1'] == 0
    mask = condition1 & condition2
    merged_df = merged_df[~mask]
    
    merged_df['State_UT'] = merged_df['State_UT'].replace('Dadra and Nagar Haveli', 'Dadra and Nagar Haveli and Daman and Diu')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Dadra & Nagar Haveli', 'Dadra and Nagar Haveli and Daman and Diu')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    merged_df['State_UT'] = merged_df['State_UT'].replace('Jammu & Kashmir', 'Jammu and Kashmir')
    
    merged_df = merged_df.groupby(['date_recorded', 'State_UT']).sum().reset_index()
    
    merged_df = merged_df[['date_recorded','State_UT','dose_1','dose_2','15_18_years_dose_1','15_18_years_dose_2','12_14_years_dose_1','12_14_years_dose_2','precaution_dose','total_doses']]
    
    merged_df.to_csv(script_path + "output/Vaccination_statewize.csv", sep = '|',index=False)
    
    print(merged_df)
    


transform_dataset_3_vaccination_3()

def transform_merge_vaccination_cases():
    
    vaccination_df = spark.read.csv(script_path + 'output/Vaccination_statewize.csv', sep='|', header=True, inferSchema=False)
    from pyspark.sql.functions import col, when, substring, sum
    final_merged_df = df_cases.\
                    join(vaccination_df, (col('date_recorded') == col('report_time')) & (col('State_UT') == col('state_name')), 'right').drop('date_recorded', 'state', 'State_UT')
    final_merged_df = final_merged_df.select('report_time', 'state_name', 'cured', 'death', 'confirmed', 
                                         'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', 
                                         '12_14_years_dose_1', '12_14_years_dose_2', 'precaution_dose', 'total_doses')
    final_merged_df.coalesce(1).write.mode("overwrite").option("header", "true").option("index", "false").csv(script_path + "output/covid_cases_vaccination_statewize.csv")
    return final_merged_df



final_statewize_covid = transform_merge_vaccination_cases()

def extract_dataset_4_pollution():
    client = bigquery.Client()
    
    # Using WHERE reduces the amount of data scanned / quota used
    query = f"""SELECT * FROM `bigquery-public-data.openaq.global_air_quality`"""
    
    query_job = client.query(query)
    
    iterator = query_job.result(timeout=30)
    rows = list(iterator)
    
    # Transform the rows into a nice pandas dataframe
    df_global_air_quality = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
    return(df_global_air_quality)


load_pollution_data = False
if(load_pollution_data):
    df_global_air_quality = extract_dataset_4_pollution()
    print(df_global_air_quality)
    df_global_air_quality.to_csv(input_path + "global_air_quality.csv", index=False)



def city_filter(value):
    if(value.find('-')>=0 and value.find(',')>=0):
        return ((value.split('-')[-2]).split(',')[-1]).strip()
    else:
        return value


def pollution_board_filter(value):
    if(value.find('-')>=0):
        return (value.split('-')[1]).strip()
    else:
        return value.strip()


def transform_dataset_4_pollution_1():
    df_global_air_quality = pd.read_csv(input_path + "global_air_quality.csv")
    df_global_air_quality.rename(columns={'country': 'country_code2'}, inplace=True)
    
    print(df_global_air_quality['timestamp'].min(), df_global_air_quality['timestamp'].max())
    
    df_indian_air_quality = df_global_air_quality[df_global_air_quality['country_code2']=="IN"]
    
    df_indian_air_quality = df_indian_air_quality[["location", "city", "country_code2", "pollutant", "value", "timestamp", "unit"]]
    
    df_indian_air_quality['timestamp'] =  pd.to_datetime(df_indian_air_quality['timestamp']).dt.strftime('%Y-%m-%d')
    
    df_indian_air_quality.info()
    
    df_indian_air_quality_pivot = pd.pivot_table(df_indian_air_quality, values = 'value', index=['country_code2', 'city', 'location', 'timestamp'], columns = 'pollutant').reset_index()
    
    df_indian_air_quality_pivot.info()
    
    df_group = df_indian_air_quality_pivot.groupby(["country_code2", "city", "location", "timestamp"]).agg({'co': np.mean,
                                                                                  'no2': np.mean,
                                                                                  'o3': np.mean,
                                                                                  'pm10': np.mean,
                                                                                  'pm25': np.mean,
                                                                                  'so2': np.mean})
    
    df_group.reset_index(inplace=True)
    
    df_group.sort_values('timestamp', ascending=False)
    
    df_group['city_refined'] = df_group.apply(lambda row: city_filter(row['location']), axis=1)
    
    df_group['pollution_board'] = df_group.apply(lambda row: pollution_board_filter(row['location']), axis=1)
    
    return df_group



df_group = transform_dataset_4_pollution_1()


def extract_dataset_5_city_state():
    df_state_city = pd.read_csv(input_path + "list_of_cities_and_towns_in_india.csv")
    return df_state_city

df_state_city = extract_dataset_5_city_state()

def transform_dataset_4_5_pollution_statewize_1():
    df_group_merged = pd.merge(df_group, df_state_city, left_on='city_refined', right_on='Name of City', how="left")
    
    df_group_merged = df_group_merged[["timestamp","co","no2","o3","pm10","pm25","so2","Name of City","State"]]
    
    final_pollution_statewize = df_group_merged.groupby(["timestamp", "State"]).agg({'co': np.mean,
                                                                              'no2': np.mean,
                                                                              'o3': np.mean,
                                                                              'pm10': np.mean,
                                                                              'pm25': np.mean,
                                                                              'so2': np.mean}).reset_index()
    
    return final_pollution_statewize


final_pollution_statewize = transform_dataset_4_5_pollution_statewize_1()

def transform_dataset_4_5_pollution_statewize_2(final_pollution_statewize):
    final_pollution_statewize = final_pollution_statewize.sort_values(['State','timestamp'], ascending=True)
    
    grouped = final_pollution_statewize.groupby('State')
    final_pollution_statewize_new = copy.deepcopy(final_pollution_statewize)
    final_pollution_statewize_new['co'] = grouped['co'].fillna(method='ffill')
    final_pollution_statewize_new['no2'] = grouped['no2'].fillna(method='ffill')
    final_pollution_statewize_new['o3'] = grouped['o3'].fillna(method='ffill')
    final_pollution_statewize_new['pm10'] = grouped['pm10'].fillna(method='ffill')
    final_pollution_statewize_new['pm25'] = grouped['pm25'].fillna(method='ffill')
    final_pollution_statewize_new['so2'] = grouped['so2'].fillna(method='ffill')
    
    final_pollution_statewize_new['co'] = grouped['co'].fillna(method='bfill')
    final_pollution_statewize_new['no2'] = grouped['no2'].fillna(method='bfill')
    final_pollution_statewize_new['o3'] = grouped['o3'].fillna(method='bfill')
    final_pollution_statewize_new['pm10'] = grouped['pm10'].fillna(method='bfill')
    final_pollution_statewize_new['pm25'] = grouped['pm25'].fillna(method='bfill')
    final_pollution_statewize_new['so2'] = grouped['so2'].fillna(method='bfill')
    
    final_pollution_statewize_new[['co', 'no2', 'o3', 'pm10', 'pm25', 'so2']] = final_pollution_statewize_new[['co', 'no2', 'o3', 'pm10', 'pm25', 'so2']].round(2)
    
    final_pollution_statewize_new.info()
    
    final_pollution_statewize_new.to_csv(script_path + "output/pollution_statewize.csv", index = False)



transform_dataset_4_5_pollution_statewize_2(final_pollution_statewize)


def transform_dataset_4_vaccination_cases_pollution():
    final_pollution_statewize = spark.read.csv(script_path + 'output/pollution_statewize.csv', sep=',', header=True, inferSchema=True)
    print(final_pollution_statewize)
    cases_vaccination_pollution_df = final_statewize_covid.\
                    join(final_pollution_statewize, (col('report_time') == col('timestamp')) & (col('state_name') == col('State')), 'left').drop('timestamp', 'State')
    cases_vaccination_pollution_df
    mean_values = cases_vaccination_pollution_df.groupBy("state_name").mean("co", "no2", "o3", "pm10", "pm25", "so2")
    # Join mean values with the original DataFrame
    filled_df = cases_vaccination_pollution_df.join(mean_values, "state_name", "left")
    # Fill null values with the corresponding mean values
    filled_df = filled_df.withColumn("co", F.coalesce(filled_df["co"], mean_values["avg(co)"]))
    filled_df = filled_df.withColumn("no2", F.coalesce(filled_df["no2"], mean_values["avg(no2)"]))
    filled_df = filled_df.withColumn("o3", F.coalesce(filled_df["o3"], mean_values["avg(o3)"]))
    filled_df = filled_df.withColumn("pm10", F.coalesce(filled_df["pm10"], mean_values["avg(pm10)"]))
    filled_df = filled_df.withColumn("pm25", F.coalesce(filled_df["pm25"], mean_values["avg(pm25)"]))
    filled_df = filled_df.withColumn("so2", F.coalesce(filled_df["so2"], mean_values["avg(so2)"]))
    filled_df = filled_df.drop("avg(co)", "avg(no2)", "avg(o3)", "avg(pm10)", "avg(pm25)", "avg(so2)")
    filled_df = filled_df.na.drop(subset=["report_time"])
    filled_df.coalesce(1).write.mode("overwrite").option("header", "true").option("index", "false").csv(script_path + "output/cases_vaccination_pollution_statewize.csv")
    return filled_df


cases_vaccination_pollution_df = transform_dataset_4_vaccination_cases_pollution()

cases_vaccination_pollution_df.sort(cases_vaccination_pollution_df.report_time).show()

def transform_statewize_to_countrywize():
    cases_vaccination_pollution_df = pd.read_csv(glob.glob(script_path + "/output/cases_vaccination_pollution_statewize.csv/" + "*.csv")[0])
    sum_columns = ['cured', 'death', 'confirmed', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2', 'precaution_dose', 'total_doses']
    avg_columns = ['co','no2','o3','pm10','pm25','so2']
    grouped_df = cases_vaccination_pollution_df.groupby('report_time').agg({
        **{col_name: 'sum' for col_name in sum_columns},
        **{col_name: 'mean' for col_name in avg_columns}}).reset_index()
    df_countrywize = spark.createDataFrame(grouped_df)
    df_countrywize.coalesce(1).write.mode("overwrite").option("header", "true").option("index", "false").csv(script_path + "output/covid_cases_vaccination_countrywize.csv")
    return df_countrywize


final_countryewize_covid = transform_statewize_to_countrywize()
final_countryewize_covid.orderBy(col("confirmed").desc()).show(truncate=False)

def extract_dataset_6_stock2():
    nifty_50 = spark.read.csv(script_path + 'input/NIFTY_50_Data.csv', sep=',', header=True, inferSchema=True)
    nifty_500 = spark.read.csv(script_path + 'input/NIFTY_500_Data.csv', sep=',', header=True, inferSchema=True)
    nifty_healthcare = spark.read.csv(script_path + 'input/NIFTY_HEALTHCARE_Data.csv', sep=',', header=True, inferSchema=True)
    nifty_IT = spark.read.csv(script_path + 'input/NIFTY_IT_Data.csv', sep=',', header=True, inferSchema=True)
    nifty_pharma = spark.read.csv(script_path + 'input/NIFTY_PHARMA_Data.csv', sep=',', header=True, inferSchema=True)
    return nifty_50, nifty_500, nifty_healthcare, nifty_IT, nifty_pharma


def extract_dataset_6_stock():
    nifty_50 = spark.sql("SELECT * FROM stock_database_covid.stock_nifty_50")
    nifty_500 = spark.sql("SELECT * FROM stock_database_covid.stock_nifty_500")
    nifty_healthcare = spark.sql("SELECT * FROM stock_database_covid.stock_nifty_healthcare")
    nifty_IT = spark.sql("SELECT * FROM stock_database_covid.stock_nifty_it")
    nifty_pharma = spark.sql("SELECT * FROM stock_database_covid.stock_nifty_pharma")


nifty_50, nifty_500, nifty_healthcare, nifty_IT, nifty_pharma = extract_dataset_6_stock2()

def transform_dataset_5_stock(nifty_50, nifty_500, nifty_healthcare, nifty_IT, nifty_pharma):
    nifty_50 = nifty_50.withColumn("Date", to_date("Date", "dd MMM yyyy"))
    nifty_50 = nifty_50.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
    #nifty_50 = nifty_50.withColumnRenamed('close', 'close_nifty_50')
    nifty_50 = nifty_50.withColumn("close_nifty_50", col("close").cast("double"))
    nifty_50 = nifty_50[['Date', 'close_nifty_50']]
    
    nifty_500 = nifty_500.withColumn("Date", to_date("Date", "dd MMM yyyy"))
    nifty_500 = nifty_500.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
    #nifty_500 = nifty_500.withColumnRenamed('close', 'close_nifty_500')
    nifty_500 = nifty_500.withColumn("close_nifty_500", col("close").cast("double"))
    nifty_500 = nifty_500[['Date', 'close_nifty_500']]
    
    nifty_healthcare = nifty_healthcare.withColumn("Date", to_date("Date", "dd MMM yyyy"))
    nifty_healthcare = nifty_healthcare.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
    #nifty_healthcare = nifty_healthcare.withColumnRenamed('close', 'close_nifty_healthcare')
    nifty_healthcare = nifty_healthcare.withColumn("close_nifty_healthcare", col("close").cast("double"))
    nifty_healthcare = nifty_healthcare[['Date', 'close_nifty_healthcare']]
    
    nifty_IT = nifty_IT.withColumn("Date", to_date("Date", "dd MMM yyyy"))
    nifty_IT = nifty_IT.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
    #nifty_IT = nifty_IT.withColumnRenamed('close', 'close_nifty_IT')
    nifty_IT = nifty_IT.withColumn("close_nifty_IT", col("close").cast("double"))
    nifty_IT = nifty_IT[['Date', 'close_nifty_IT']]
    
    nifty_pharma = nifty_pharma.withColumn("Date", to_date("Date", "dd MMM yyyy"))
    nifty_pharma = nifty_pharma.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
    #nifty_pharma = nifty_pharma.withColumnRenamed('close', 'close_nifty_pharma')
    nifty_pharma = nifty_pharma.withColumn("close_nifty_pharma", col("close").cast("double"))
    nifty_pharma = nifty_pharma[['Date', 'close_nifty_pharma']]
    
    joined_stock_df = nifty_50.join(nifty_500, "Date","left").join(nifty_healthcare, "Date","left").join(nifty_IT, "Date","left").join(nifty_pharma, "Date","left")
    joined_stock_df = joined_stock_df.orderBy('Date')
    
    # Define window specification for filling null values
    windowSpec = Window.orderBy('Date').rowsBetween(Window.unboundedPreceding, 0)
    
    # Fill null values with previous non-null values
    filled_df = joined_stock_df.select('Date', \
    F.last('close_nifty_50', True).over(windowSpec).alias('close_nifty_50'), \
    F.last('close_nifty_500', True).over(windowSpec).alias('close_nifty_500'), \
    F.last('close_nifty_healthcare', True).over(windowSpec).alias('close_nifty_healthcare'), \
    F.last('close_nifty_IT', True).over(windowSpec).alias('close_nifty_IT'), \
    F.last('close_nifty_pharma', True).over(windowSpec).alias('close_nifty_pharma'))
    
    return joined_stock_df


joined_stock_df = transform_dataset_5_stock(nifty_50, nifty_500, nifty_healthcare, nifty_IT, nifty_pharma)
joined_stock_df.orderBy(col("Date")).show()

final_countryewize_covid_df = final_countryewize_covid.join(joined_stock_df, (col('report_time') == col('Date')),"left").drop('Date')

windowSpec = Window.orderBy('report_time').rowsBetween(Window.unboundedPreceding, 0)
final_countryewize_covid_df = final_countryewize_covid_df.select('report_time', 'cured', 'death', 'confirmed', 'dose_1', 'dose_2', '15_18_years_dose_1', '15_18_years_dose_2', '12_14_years_dose_1', '12_14_years_dose_2', 'precaution_dose', 'total_doses', 'co', 'no2', 'o3',	'pm10', 'pm25', 'so2', \
    F.last('close_nifty_50', True).over(windowSpec).alias('close_nifty_50'), \
    F.last('close_nifty_500', True).over(windowSpec).alias('close_nifty_500'), \
    F.last('close_nifty_healthcare', True).over(windowSpec).alias('close_nifty_healthcare'), \
    F.last('close_nifty_IT', True).over(windowSpec).alias('close_nifty_IT'), \
    F.last('close_nifty_pharma', True).over(windowSpec).alias('close_nifty_pharma'))

final_countryewize_covid_df.coalesce(1).write.mode("overwrite").option("header", "true").option("index", "false").csv(script_path + "output/covid_cases_vaccination_pollution_stock_countrywize.csv")




#Analysis Queries:
#1. How different stocks performed during corona period. Which industry gained more profits during corona and which industry incurred losses during corona?
pandas_df = joined_stock_df.select("Date", "close_nifty_50", "close_nifty_500",
                                   "close_nifty_healthcare", "close_nifty_IT",
                                   "close_nifty_pharma").toPandas()
numeric_columns = ['close_nifty_50', 'close_nifty_500', 'close_nifty_healthcare', 'close_nifty_IT', 'close_nifty_pharma']
sns.lineplot(x="Date", y="value", hue="variable", data=pd.melt(pandas_df, ["Date"]))
ax.set_xlabel("Date")
ax.set_ylabel("Performance")
plt.savefig(script_path + "output/plot_1_stock_1.png")


pandas_df["Quarter"] = pd.to_datetime(pandas_df["Date"]).dt.to_period("Q")
pandas_df.plot(x="Quarter", y=numeric_columns, kind="line", figsize=(10, 10))
ax.set_xlabel("Quarter")
ax.set_ylabel("Performance")
plt.savefig(script_path + "output/plot_1_stock_2.png")

#2. How environment was affected during corona period?

pandas_df2 = cases_vaccination_pollution_df.select("report_time", "confirmed").toPandas()
pandas_df2 = pandas_df2.sort_values("report_time")
numeric_columns = ["confirmed"]
pandas_df2["Quarter"] = pd.to_datetime(pandas_df2["report_time"]).dt.to_period("Q")
pandas_df2.plot(x="report_time", y="confirmed", kind="line", figsize=(10, 10))
ax.set_xlabel("Date")
ax.set_ylabel("Confirmed")
plt.savefig(script_path + "output/plot_2_confirmed_cases_1.png")

pandas_df3 = cases_vaccination_pollution_df.select("report_time", "pm25").toPandas()
pandas_df3 = pandas_df3.sort_values("report_time")
numeric_columns = ["confirmed"]
pandas_df3["Quarter"] = pd.to_datetime(pandas_df3["report_time"]).dt.to_period("Q")
pandas_df3.plot(x="report_time", y="pm25", kind="line", figsize=(10, 10))
ax.set_xlabel("Date")
ax.set_ylabel("pollution")
plt.savefig(script_path + "output/plot_2_pollution_2.png")

#3. How "Rapid vaccination program" slowed down corona?

pandas_df4 = cases_vaccination_pollution_df.select("report_time", "total_doses", "confirmed").toPandas()
pandas_df4['report_time'] = pd.to_datetime(pandas_df4['report_time'])
pandas_df4 = pandas_df4.sort_values("report_time")
sns.lineplot(data=pandas_df4, x='report_time', y='total_doses', label='total_doses')
sns.lineplot(data=pandas_df4, x='report_time', y='confirmed', label='confirmed')
plt.title('Plot of Vaccination and cases')
plt.xlabel('Date')
plt.ylabel('Vaccination_cases')
plt.savefig(script_path + "output/plot_3_cases_vaccination.png")

#4. When was corona related deaths at pick?

pandas_df5 = cases_vaccination_pollution_df.select("report_time", "death").toPandas()
pandas_df5['report_time'] = pd.to_datetime(pandas_df5['report_time'])
pandas_df5 = pandas_df5.sort_values("report_time")
pandas_df5["Quarter"] = pd.to_datetime(pandas_df5["report_time"]).dt.to_period("Q")
sns.lineplot(data=pandas_df5, x='Quarter', y='death')

plt.title('Plot of death over report_time')
plt.xlabel('Report Time')
plt.ylabel('Death Count')
plt.savefig(script_path + "output/plot_4_deaths.png")

#5. Which state was affected most and least by corona?

pandas_df6 = df_cases.select("state_name", "death").toPandas()
state_deaths = pandas_df6.groupby('state_name')['death'].sum()
state_with_highest_deaths = state_deaths.idxmax()
state_with_lowest_deaths = state_deaths.idxmin()
print("state_with_highest_deaths =", state_with_highest_deaths, "Deaths = ", state_deaths[state_with_highest_deaths])
print("state_with_lowest_deaths =", state_with_lowest_deaths, "Deaths = ", state_deaths[state_with_lowest_deaths])

state_deaths = state_deaths.reset_index(drop=False)
state_deaths = state_deaths.sort_values('death', ascending=False)
sns.barplot(data=state_deaths, x='state_name', y='death')


plt.title('Total Deaths by State')
plt.xlabel('State')
plt.ylabel('Total Deaths')
plt.xticks(rotation=45)
plt.savefig(script_path + "output/plot_5_state_deaths.png")


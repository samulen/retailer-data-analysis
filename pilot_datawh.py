"""
Created on Sat Dec 30 18:51:13 2023

@author: SL01

Pilot of the datamart creation


"""
import constants as c

import numpy as np 
import pandas as pd
from pandasql import sqldf
import datetime as dt

import logging
import mysql.connector

import mySqlUtility as mysql_m



def import_data():
    try:
        connection = mysql.connector.connect(host='localhost',
                                            database=c.database_name,
                                            user=c.database_username,
                                            password=c.database_password)

        sql_select_Query = f"""SELECT a.*, nome, indirizzo_1, città, provincia, nazione, c.*
                             FROM {c.schema}.orders a
                             JOIN {c.schema}.customers b ON a.customer_id = b.customer_id
                             JOIN {c.schema}.inventory c ON a.id_modello = c.id_modello"""
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        column_names = [i[0] for i in cursor.description]
        records = cursor.fetchall()
        dataset = pd.DataFrame(records, columns = column_names).drop(columns='id_modello')
        
        return dataset
        
    except mysql.connector.Error as e:
        logging.ERROR("Error reading data from MySQL table", e)
    
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()


def enrich_dataset(dataset):
    #Geo-demographics
    raw_regionAndInhabitants = pd.read_excel("data\\Elenco-comuni-italiani.xls", 
                                            usecols = ['Ripartizione geografica', 
                                                        'Denominazione regione', 
                                                        'city', 
                                                        'Sigla automobilistica',
                                                        'Popolazione legale 2011 (09/10/2011)'])
    
    renamed_regionAndInhabitants = raw_regionAndInhabitants.rename(columns={'Ripartizione geografica':'area', 
                                                                            'Denominazione regione':'region', 
                                                                            'Sigla automobilistica':'provincia',
                                                                            'Popolazione legale 2011 (09/10/2011)':'population'
                                                                            })
    
    grouped_regionAndInhabitants = renamed_regionAndInhabitants.groupby(['area', 'region', 'city', 'provincia'])['population'].sum().reset_index()
    grouped_regionAndInhabitants['nazione'] ='IT'

    dataset = pd.merge(dataset, grouped_regionAndInhabitants, how='left', on=['provincia', 'nazione'] )

    #Add time column, to set as index and fill with missing days
    dataset['date'] = dataset['datetime'].dt.date
    timespan = pd.DataFrame({'date':pd.date_range(dataset['date'].min(), dataset['date'].max(), freq='D')})

    # Merge the original DataFrame with the calendar DataFrame to fill missing dates
    result_df = pd.concat(timespan, dataset)
    print(result_df)
    # Fill missing values in 'name' column with 0
    dataset_DateIndexed = result_df['name'].fillna(0)

    dataset_DateIndexed['year'] = dataset_DateIndexed['date'].dt.year
    dataset_DateIndexed['month'] = dataset_DateIndexed['date'].dt.month
    dataset_DateIndexed['day'] = dataset_DateIndexed['date'].dt.day
    dataset_DateIndexed['hour'] = dataset_DateIndexed['datetime'].dt.hour
    dataset_DateIndexed['weekday'] = dataset_DateIndexed['date'].dt.strftime('%A') 
    #mysql_m.append_df_to_table(dataset, 'dataset')
    return dataset_DateIndexed

def customer_behavior_analysis(dataset):
    #Geographical/historical analyisis
    time_behavior_by_region = dataset.groupby(['nazione', 'area', 'year', 'month', 'day', 'hour', 'weekday'])['ID']  \
                                    .count()   \
                                    .reset_index()
    print(time_behavior_by_region)
    """
    #Repurchases
    """
    #Identify clients that repurchased
    repo_custs = dataset.loc[dataset['Item'] == 0, ['Customer_ID', 'ID']].groupby('Customer_ID')  \
                                                                         .agg(orders_qty=('ID', 'count'))  \
                                                                         .reset_index()                                                           
    repo_custs = repo_custs.loc[repo_custs['orders_qty'] > 1, 'Customer_ID'].to_numpy()
    orders_repo_custs = dataset[dataset['Customer_ID'].isin(repo_custs)]

    #What is the average time of repurchases?
    repo_time_sorted = orders_repo_custs.loc[orders_repo_custs['Item']==0, ['Customer_ID', 'date']] \
                                        .sort_values(by=['Customer_ID', 'date'])
    print(repo_time_sorted)
    repo_time_sorted['n_days'] = repo_time_sorted.groupby('Customer_ID')['date'].diff().dt.days 
    repo_freq = repo_time_sorted.dropna()
    print(repo_freq)
    #repo_time_sorted['avg'] = repo_freq.groupby('Customer_ID').agg(mean_frequency=('n_days', 'mean'))

    repos_habits = pd.merge(orders_repo_custs,  repo_freq, on=['Customer_ID', 'date'], how = 'left')
    print(repos_habits)

    #How many as a percentage of the total? by region, month and year
    tbl1 = repos_habits[['Customer_ID', 'city', 'nazione', 'n_days']] 
    tbl2 = dataset[['Customer_ID', 'area','city', 'nazione', 'year', 'month']]
    qry_repos = """
                SELECT b.area, b.nazione, b.city, b.month, b.year, 
                       AVG(a.n_days), COUNT(DISTINCT A.Customer_ID), COUNT(DISTINCT B.Customer_ID),
                       COUNT(DISTINCT a.Customer_ID)/COUNT(DISTINCT B.Customer_ID) as pctRepos 
                FROM tbl1 a 
                 RIGHT JOIN tbl2 b
                 ON a.Customer_ID = b.Customer_ID
                GROUP BY b.nazione, b.city, b.month, b.year
                """
    repoOnTotal = sqldf(qry_repos)
    
    print(repoOnTotal)
    mysql_m.replace_df(time_behavior_by_region, 'weekdays')
    mysql_m.replace_df(repos_habits.drop(columns='id_modello'), 'repurchases')
    mysql_m.replace_df(repoOnTotal, 'ratios')
    #Which are the regions/areas with the most sales compared to the population
    


    #Refunds and size errors analysis

    #Product - Model and Sizes analysis


    #Rebuy habits, how many in proportion and average rebuy time, combinations of models
    #Multiple items per order
    double = dataset['Item'] > 0
    

    #Geographical behavior

    #Retrieve number of inhabitants and region if Italy 

    #

    #Nazione


def main():
    dataset_raw = import_data()
    dataset = enrich_dataset(dataset_raw)
    #Prepare
    customer_behavior_analysis(dataset)

    #performance_analysis(dataset)

    
if __name__ == "__main__":
    main()
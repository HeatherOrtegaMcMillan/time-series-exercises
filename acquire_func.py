########################### TIME SERIES ACQUIRE ###########################
#### Only the functions 

# import modules
import pandas as pd 
import numpy as np 
import requests
import os
def get_sales_data(sales_url = 'https://python.zach.lol/api/v1/sales', 
                    endpoint = 'sales'):
    '''
    This function reads in the sales data from the zach api,
    writes data to a csv file if a local file does not exist, 
    and returns a df.
    '''

    if os.path.isfile('sales.csv'):
        
        # If csv file exists read in data from csv file.
        df = pd.read_csv('sales.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame
        df = get_api_pages(sales_url, endpoint)
        
        # Cache data
        df.to_csv('sales.csv')

    return df

def get_full_zach_data():
    ''' 
    This function gets the sales data, the items data, and the stores
    data. Joins them together into a single dataframe. And returns that
    dataframe
    '''
    base_url = 'https://python.zach.lol/api/v1/'

    endpoint_list = ['sales', 'items', 'stores']

    sales_df = get_sales_data()

    items_df = get_api_pages(base_url + 'items', 'items')

    stores_df = get_api_pages(base_url + 'stores', 'stores')
    
    return sales_df, items_df, stores_df

def join_zach_data(df1, df2, df3):
    '''
    This function takes in three tuples with the dataframe and the 
    key for that dataframe to be joined on (i.e. (df, 'key'))
    Returns one dataframe with them all left joined together. 
    Df1 needs two keys! for the first join and the second join
    Joins df1 to df2, then those two to df3
    i.e. join_zach_data((sales_df, 'item', 'store'), 
    (items_df, 'item_id'), (stores_df, 'store_id'))
    '''
    
    # Merge df1 and 2, set the right table index to the id of that column, 
    # then use the right index for the join
    join_1 = df1[0].merge(df2[0].set_index(df2[1]), how='left', left_on = df1[1], right_index = True)
    
    # merge the joined df to df3
    join_full = join_1.merge(df3[0].set_index(df3[1]), how = 'left', left_on= df1[2], right_index = True )
    
    return join_full

def the_whole_shebang():
    '''
    This function does a whole thing with getting the zach data specifically
    Needs the other functions in this file to work
    '''
    # get all three dataframes using get full function
    sales_df, items_df, stores_df = get_full_zach_data()
    
    # join the dataframes together using the join function
    all_sales_data = join_zach_data((sales_df, 'item', 'store'), (items_df, 'item_id'), (stores_df, 'store_id'))
    
    return all_sales_data


########################################################################
#################### UNIVERSAL DATAFRAME API GETTER ####################

# The 3 functions below can be used to get any amount of dataframes
# You can copy this into any acquire script

def get_api_pages(base_url, endpoint):
    ''' 
    This function takes in a base url, a string of the thing you want
    Creates a dataframe of all the endpoint's pages in a dataframe
    The endpoint aka the "thing string"
    ex. get_api_pages('https://python.zach.lol/api/v1/sales', 'sales')
    '''
    response = requests.get(base_url)
    data = response.json()
    
    maxpage = data['payload']['max_page'] #set maxpage to the payload's max_page
    
    items_list = [] # initialize list to store pages

    for page in range(1, maxpage + 1):
        url = base_url + '?page=' + str(page)
        response = requests.get(url)
        page_data = response.json()
        page_items = page_data['payload'][endpoint]
        items_list += page_items
    
    return pd.DataFrame(items_list)

def get_api_data(base_url, endpoint):
    '''
    This function reads in the data from an api.
    If there's a csv of that file it reads from there. if not it creates one
    uses get_api_pages function
    '''

    if os.path.isfile(f'{endpoint}.csv'):
        
        # If csv file exists read in data from csv file.
        df = pd.read_csv(f'{endpoint}.csv')
        
    else:
        
        # Read fresh data from db into a DataFrame
        df = get_api_pages(base_url, endpoint)
        
        # Cache data
        df.to_csv(f'{endpoint}.csv', index=False)

    return df

####### This is the function you will need to run
####### Will need a url and a list of your endpoints

def get_all_dataframes_from_api(base_url, endpoint_list):
    '''
    This function takes in a url and an endpoint list
    Endpoint list is list of strings 
    returns a tuple with all the dataframes in it
    will need : base_url = 'https://python.zach.lol/api/v1/'
    endpoint_list = ['sales', 'items', 'stores']
    '''
    df_list = [] # initalize dataframe list
    
    for endpoint in endpoint_list: # loop through endpoint list that was entered
    
        # use exec() function to execute a formatted string
        # run through get api data function
        exec(f"{endpoint}_df = get_api_data(base_url + '{endpoint}', '{endpoint}')")
        
        # use exec() to append new dataframe to list
        exec(f"df_list.append({endpoint}_df)")
    
    return tuple(df_list)



###################### Acquire for Germany Data ########################
def get_germany_data():
    '''
    This function checks to see if there's a cached csv of the opsd data
    If not it reads the data from the API
    Returns the dataframe ready for cleaning
    '''
    if os.path.isfile('opsd_germany.csv'):

        opsd_germany = pd.read_csv('opsd_germany.csv')

    else:
        opsd_germany = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        opsd_germany.to_csv('opsd_germany.csv', index=False)

    return opsd_germany
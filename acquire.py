########################### TIME SERIES ACQUIRE ###########################
# import modules
import pandas as pd 
import numpy as np 
import requests
import os

################# Exercise 1 #################
# Using the code from the lesson as a guide and the REST API 
# from https://python.zach.lol/api/v1/items as we did in the lesson, 
# create a dataframe named items that has all of the data for items.

items_url = 'https://python.zach.lol/api/v1/items'
response = requests.get(items_url)
data = response.json()

#set maxpage to the payload's max_page
maxpage = data['payload']['max_page']

# initialize list to store pages
items_list = []
# loop through next pages to get them all together
# need maxpage + 1 because of the way range works
for page in range(1, maxpage+1):
    url = items_url + '?page =' + str(page)
    response = requests.get(url)
    data = response.json()
    page_items = data['payload']['items']
    items_list += page_items

item_df = pd.DataFrame(items_list)


################# Exercise 2 #################
# Do the same thing, but for stores (https://python.zach.lol/api/v1/stores)
stores_url = 'https://python.zach.lol/api/v1/stores'

response = requests.get(stores_url)

data = response.json()

stores_df = pd.DataFrame(data['payload']['stores'])


################# Exercise 3 #################
#Extract the data for sales (https://python.zach.lol/api/v1/sales). 
# There are a lot of pages of data here, so your code will need to be a 
# little more complex. Your code should continue fetching data from the 
# next page until all of the data is extracted.

sales_url = 'https://python.zach.lol/api/v1/sales'

def get_api_pages(base_url, endpoint):
    ''' 
    This function takes in a base url, a string of the thing you want
    Creates a dataframe of all the endpoint's pages in a dataframe
    Endpoint aka thing string
    ex. get_api_pages('https://python.zach.lol/api/v1/sales', 'sales')
    '''
    response = requests.get(base_url)
    data = response.json()
    
    maxpage = data['payload']['max_page'] #set maxpage to the payload's max_page
    
    items_list = [] # initialize list to store pages

    for page in range(1, maxpage+1):
        url = base_url + '?page =' + str(page)
        response = requests.get(url)
        page_data = response.json()
        page_items = page_data['payload'][thing_string]
        items_list += page_items
    
    return pd.DataFrame(items_list)

sales_df = get_api_pages(sales_url, 'sales')

################# Exercise 4 #################
# Save the data in your files to local csv files so that it will be 
# faster to access in the future.






################# Exercise 5 #################
# Combine the data from your three separate dataframes into one large dataframe.







################# Exercise 6 #################
# Acquire the Open Power Systems Data for Germany, which has been rapidly 
# expanding its renewable energy production in recent years. The data set 
# includes country-wide totals of electricity consumption, wind power 
# production, and solar power production for 2006-2017. 
# You can get the data here:
# https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv



################# Exercise 7 #################
# Make sure all the work that you have done above is reproducible. 
# That is, you should put the code above into separate functions in the 
# acquire.py file and be able to re-run the functions and get the same data.



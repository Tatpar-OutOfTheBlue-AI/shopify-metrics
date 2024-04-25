import requests
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
from collections import defaultdict
import numpy as np
import copy

url = 'https://kavi-pushp.myshopify.com/admin/api/2024-04/graphql.json'
headers = {
    'Content-Type': 'application/json',
    "X-Shopify-Access-Token": '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'
}

def getGraphQLData(query, variables=None):
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    response = requests.post(url, json=payload, headers=headers)
    return response.json()


def paginate_query(query_template):
    cursor = None
    has_next_page = True
    data = []
    
    while has_next_page:
        variables = {'cursor': cursor}  # Define cursor variable
        response = getGraphQLData(query_template, variables)
        edges = response['data']['customers']['edges']
        data.extend([edge['node'] for edge in edges])
        
        has_next_page = response['data']['customers']['pageInfo']['hasNextPage']
        if edges:
            cursor = edges[-1]['cursor']
    
    return data

def getCustomerMetrics(dimensions):
  
  customers_query_template = """
    query($cursor: String) {
      customers(first: 250, after: $cursor) {
        edges {
          node {
            addresses {
              city
              company
              country
              countryCodeV2
              province
              provinceCode
              timeZone
              zip
            }
            amountSpent {
              amount
              currencyCode
            }
            defaultAddress {
              city
              company
              country
              countryCodeV2
              province
              provinceCode
              timeZone
              zip
            }
            emailMarketingConsent {
              marketingOptInLevel
              marketingState
            }
            lastOrder {
              createdAt
            }
            lifetimeDuration
            numberOfOrders
            productSubscriberStatus
            smsMarketingConsent {
              marketingOptInLevel
              marketingState
            }
            createdAt
            firstName
            lastName
            state
            statistics {
              predictedSpendTier
            }
            tags
            taxExempt
            taxExemptions
            verifiedEmail
          }
          cursor
        }
        pageInfo {
          hasNextPage
        }
      }
    }
  """

  customers_data = paginate_query(customers_query_template)

  customers_data_transformed = transform_datewise(customers_data, 'date_hour')

  with open('customer_graphQl.json', 'w') as file:
    json.dump(customers_data_transformed, file, indent=4)
  
  churn_condition = 12 #months

  metrics = {}
  for date in list(customers_data_transformed.keys()):
        date_str = date
        dimensions_wise_data = create_dimension_data(dimensions)
        
        metrics[date_str] = {
           "total_customers": {
                "total_value": customers_data_transformed[date_str]['count'],
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "new_customers": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_amount_spent": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_orders": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "repeat_customers": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "churn_customers": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "churn_rate": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "repeat_customers_percentage": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_order_value": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "purchase_frequency": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            }
        }

        for customer in customers_data_transformed[date_str]['data']:
           metrics = init_dimension_data(dimensions, metrics, date_str, customer)
           if int(customer['numberOfOrders']) == 1:
            metrics[date_str]['new_customers']['total_value'] += 1
           if int(customer['numberOfOrders']) > 1:
            metrics[date_str]['repeat_customers']['total_value'] += 1
           
           #check if customer is a churn
            if 'lastOrder.createdAt' in customer:
              last_order_date = datetime.fromisoformat(customer['lastOrder.createdAt'].rstrip('Z')).replace(tzinfo=timezone.utc)

              current_datetime = datetime.now(timezone.utc)
              difference = relativedelta(current_datetime, last_order_date)
              total_months = difference.years * 12 + difference.months

              if total_months > churn_condition:
                metrics[date_str]['churn_customers']['total_value'] += 1
                 
              
           metrics[date_str]['repeat_customers_percentage']['total_value'] = (metrics[date_str]['repeat_customers']['total_value'] / metrics[date_str]['total_customers']['total_value']) * 100
           metrics[date_str]['total_amount_spent']['total_value'] += float(customer['amountSpent.amount'])
           metrics[date_str]['total_orders']['total_value'] += int(customer['numberOfOrders'])

           if (metrics[date_str]['total_orders']['total_value'] != 0):
            metrics[date_str]['average_order_value']['total_value'] = metrics[date_str]['total_amount_spent']['total_value'] / metrics[date_str]['total_orders']['total_value']
           else:
            metrics[date_str]['average_order_value']['total_value'] = 0
           metrics[date_str]['purchase_frequency']['total_value'] = metrics[date_str]['total_orders']['total_value'] / metrics[date_str]['total_customers']['total_value']
           metrics[date_str]['churn_rate']['total_value'] = (metrics[date_str]['churn_customers']['total_value'] / metrics[date_str]['total_customers']['total_value']) * 100

           
        for dim in dimensions:
            for customer in customers_data_transformed[date_str]['data']:
              dict_key = dim.replace('.', '_') + "_wise"
              if dim in customer:
                dim_value = customer[dim]
                metrics[date_str]["total_customers"]['data'][dict_key][dim_value] += 1
                if int(customer['numberOfOrders']) == 1:
                    metrics[date_str]["new_customers"]['data'][dict_key][dim_value] += 1
                if int(customer['numberOfOrders']) > 1:
                    metrics[date_str]["repeat_customers"]['data'][dict_key][dim_value] += 1
                
                #check if customer is a churn
                if 'lastOrder.createdAt' in customer:
                    last_order_date = datetime.fromisoformat(customer['lastOrder.createdAt'].rstrip('Z')).replace(tzinfo=timezone.utc)

                    current_datetime = datetime.now(timezone.utc)
                    difference = relativedelta(current_datetime, last_order_date)
                    total_months = difference.years * 12 + difference.months

                    if total_months > churn_condition:
                      metrics[date_str]["churn_customers"]['data'][dict_key][dim_value] += 1
                
                       
                metrics[date_str]["repeat_customers_percentage"]['data'][dict_key][dim_value] += (metrics[date_str]["repeat_customers"]['data'][dict_key][dim_value] / metrics[date_str]["total_customers"]['data'][dict_key][dim_value]) * 100
                metrics[date_str]["total_amount_spent"]['data'][dict_key][dim_value] += float(customer['amountSpent.amount'])
                metrics[date_str]["total_orders"]['data'][dict_key][dim_value] += int(customer['numberOfOrders'])
                if (metrics[date_str]["average_order_value"]['data'][dict_key][dim_value] !=0 ):
                  metrics[date_str]["average_order_value"]['data'][dict_key][dim_value] = metrics[date_str]["total_amount_spent"]['data'][dict_key][dim_value] / metrics[date_str]["total_orders"]['data'][dict_key][dim_value]
                else:
                  metrics[date_str]["average_order_value"]['data'][dict_key][dim_value] = 0
                metrics[date_str]["purchase_frequency"]['data'][dict_key][dim_value] = metrics[date_str]["total_orders"]['data'][dict_key][dim_value] / metrics[date_str]["total_customers"]['data'][dict_key][dim_value]
                metrics[date_str]["churn_rate"]['data'][dict_key][dim_value] = metrics[date_str]["churn_customers"]['data'][dict_key][dim_value] / metrics[date_str]["total_customers"]['data'][dict_key][dim_value]

              else:
                dim_value = 'null'
  
  with open('customer__graphQl_metrics.json', 'w') as file:
        json.dump(metrics, file, indent=4)

  return metrics

def init_dimension_data(dimensions, metrics, date_str, customer):
    for dim in dimensions:
      dict_key = dim.replace('.', '_') + "_wise"
      if dim in customer:
        dim_value = customer[dim]
        metrics[date_str]["total_customers"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["new_customers"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["total_orders"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["repeat_customers"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["churn_customers"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["repeat_customers_percentage"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["total_amount_spent"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["average_order_value"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["purchase_frequency"]['data'][dict_key][dim_value] = 0 
        metrics[date_str]["churn_rate"]['data'][dict_key][dim_value] = 0 
      else:
        dim_value = 'null'
    
    return metrics

def create_dimension_data(dimensions):
    dimension_data = {}
    for dim in dimensions:
        key = dim.replace('.', '_') + "_wise"
        dimension_data[key] = {}
    return dimension_data

def transform_datewise(data, group_by='date'):
    date_format = "%Y-%m-%dT%H:%M:%S%z"
    strftime_format = '%Y-%m-%d %H' if group_by == 'date_hour' else '%Y-%m-%d'

    datewise_data = defaultdict(lambda: {"count": 0, "data": []})

    for item in data:
      df_item = pd.json_normalize(item)
      df_item = df_item.replace({np.nan: None})
      json_item = df_item.to_dict(orient='records')

      date = datetime.strptime(json_item[0]['createdAt'], date_format)
      formatted_date = date.strftime(strftime_format)

      datewise_data[formatted_date]['count'] += 1
      datewise_data[formatted_date]['data'].append(json_item[0]) 
   
    return datewise_data


getCustomerMetrics(['defaultAddress.city', 'defaultAddress.company', 'defaultAddress.country', 'defaultAddress.countryCodeV2', 'defaultAddress.province', 'defaultAddress.provinceCode', 'defaultAddress.timeZone', 'defaultAddress.zip', 'emailMarketingConsent.marketingOptInLevel', 'emailMarketingConsent.marketingState', 'smsMarketingConsent.marketingOptInLevel', 'smsMarketingConsent.marketingState', 'productSubscriberStatus', 'lifetimeDuration', 'verifiedEmail', 'taxExempt', ])
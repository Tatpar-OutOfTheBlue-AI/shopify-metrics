import requests
import json
from datetime import datetime
import pandas as pd
from collections import defaultdict
import numpy as np
import copy

shop_url = 'kavi-pushp.myshopify.com'
url = 'https://kavi-pushp.myshopify.com/admin/api/2021-10/graphql.json'
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


def paginate_query(start_date, end_date, query_template):
    cursor = None
    has_next_page = True
    data = []
    
    while has_next_page:
        variables = {'cursor': cursor}  # Define cursor variable
        query = query_template.replace('{start_date}', start_date).replace('{end_date}', end_date)
        response = getGraphQLData(query, variables)
        edges = response['data']['orders']['edges']
        data.extend([edge['node'] for edge in edges])
        
        has_next_page = response['data']['orders']['pageInfo']['hasNextPage']
        if edges:
            cursor = edges[-1]['cursor']
    
    return data

def getOrdersMetrics(start_date, end_date, dimensions):

  orders_query_template = """
    query($cursor: String) {
      orders(first: 250, after: $cursor, query: "created_at:>'{start_date}' AND created_at:<'{end_date}'") {
        edges {
          node {
            id
            billingAddress {
              city
              country
              province
              zip
            }
            cancelReason
            cancelledAt
            channelInformation {
              app {
                title
              }
              channelDefinition {
                handle
                channelName
              }
            }
            confirmed
            createdAt
            currencyCode
            currentTaxLines {
              rate
              ratePercentage
              title
              price
            }
            shippingLine {
              discountAllocations {
                allocatedAmountSet {
                  shopMoney {
                    amount
                  }
                }
              }
            }
            customer {
              id
              amountSpent {
                amount
              }
              state
              statistics {
                predictedSpendTier
              }
              tags
              taxExempt
              taxExemptions
              verifiedEmail
              numberOfOrders
            }
            customerAcceptsMarketing
            customerJourneySummary{
              customerOrderIndex
              daysToConversion
              firstVisit {
                landingPage
                referrerUrl
                source
                sourceType
                referralCode
                marketingEvent {
                  utmCampaign
                  utmMedium
                  utmSource
                  sourceAndMedium
                  app{
                    title
                    handle
                  }
                }
                utmParameters{
                  campaign
                  content
                  medium
                  source
                  term
                }
              }
              lastVisit{
                landingPage
                referrerUrl
                source
                sourceType
                referralCode
                marketingEvent {
                  utmCampaign
                  utmMedium
                  utmSource
                  sourceAndMedium
                  app{
                    title
                    handle
                  }
                }
                utmParameters{
                  campaign
                  content
                  medium
                  source
                  term
                }
              }
            }
            discountCodes
            displayFinancialStatus
            displayFulfillmentStatus
            subtotalLineItemsQuantity
            fullyPaid
            note
            paymentGatewayNames
            totalRefundedShippingSet {
              shopMoney {
                amount
              }
            }
            refunds {
              totalRefunded{
                amount
              }
            }
            registeredSourceUrl
            returnStatus
            test
            totalCapturableSet {
              shopMoney {
                amount
              }
            }
            originalTotalPriceSet {
              shopMoney {
                amount
              }
            }
            subtotalPriceSet {
              shopMoney {
                amount
              }
            }
            totalDiscountsSet {
              shopMoney {
                amount
              }
            }
            totalOutstandingSet {
              shopMoney {
                amount
              }
            }
            totalPriceSet {
              shopMoney {
                amount
              }
            }
            totalReceivedSet {
              shopMoney {
                amount
              }
            }
            totalRefundedSet {
              shopMoney {
                amount
              }
            }
            totalShippingPriceSet {
              shopMoney {
                amount
              }
            }
            totalTaxSet {
              shopMoney {
                amount
              }
            }
            totalTipReceivedSet {
              shopMoney {
                amount
              }
            }
            unpaid
          }
          cursor
        }
        pageInfo {
          hasNextPage
        }
      }
    }
    """

  abandoned_checkout_data = requests.get(f"https://{shop_url}/admin/api/2024-04/checkouts.json?limit=250&created_at_min={start_date}&created_at_max={end_date}", headers=headers)

  orders_data = paginate_query(start_date, end_date, orders_query_template)

  orders_data_transformed = transform_datewise(orders_data, 'date_hour', "GraphQLAPI")
  
  abandoned_checkout_data_transformed = transform_datewise(abandoned_checkout_data.json()['checkouts'], 'date_hour', 'RestAPI')
  
  with open('order_graphQl_abandoned_chekouts.json', 'w') as file:
      json.dump(abandoned_checkout_data_transformed, file, indent=4)

  with open('order_graphQl.json', 'w') as file:
      json.dump(orders_data_transformed, file, indent=4)

  total_visitors = 350

  date_range = pd.date_range(start=start_date, end=end_date, freq='h')

  customer_data = []
  
  metrics = {}
  for date in date_range:
        date_str = date.strftime('%Y-%m-%d %H')
        dimensions_wise_data = create_dimension_data(dimensions)
        
        metrics[date_str] = {
            "total_orders": {
                "total_value": orders_data_transformed[date_str]['count'],
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_units": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_refunds": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "units_per_transaction": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_sales": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_subtotal": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "net_sales": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_discounts_amt": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_shipping_discounts_amt": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_discounts": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_discounts_amt": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_refunds_amt": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "refunds_rate": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_shipping_refunds": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_tax": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_tip_received": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "total_shipping_charges": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "orders_with_shipping_discount": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_shipping_charges": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_order_value": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "average_shipping_discount": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "cart_abandonment_rate": {
                "total_value": 0,
            },
            "conversion_rate": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "revenue_per_visitor": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "new_customers": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
            "repeat_customers": {
                "total_value": 0,
                "data": copy.deepcopy(dimensions_wise_data)
            },
        }

        for order in orders_data_transformed[date_str]['data']:
           metrics = init_dimension_data(dimensions, metrics, date_str, order)
           metrics[date_str]['total_units']['total_value'] += order['subtotalLineItemsQuantity']
           metrics[date_str]['total_sales']['total_value'] += float(order['totalPriceSet.shopMoney.amount'])
           if (order['displayFinancialStatus'] == 'REFUNDED'):
              metrics[date_str]['total_refunds']['total_value'] += 1
           metrics[date_str]['total_discounts_amt']['total_value'] += float(order['totalDiscountsSet.shopMoney.amount'])
           if len(order['shippingLine.discountAllocations']) > 0:
            metrics[date_str]['orders_with_shipping_discount']['total_value'] += 1
            for shipping_discount in order['shippingLine.discountAllocations']:
              metrics[date_str]["total_shipping_discounts_amt"]["total_value"] += float(shipping_discount['allocatedAmountSet']['shopMoney']['amount'])

           metrics[date_str]['total_subtotal']['total_value'] += float(order['subtotalPriceSet.shopMoney.amount'])
           metrics[date_str]['average_discounts']['total_value'] = (metrics[date_str]['total_discounts_amt']['total_value'] / metrics[date_str]['total_subtotal']['total_value']) * 100
           metrics[date_str]['average_discounts_amt']['total_value'] = metrics[date_str]['total_discounts_amt']['total_value'] / metrics[date_str]['total_subtotal']['total_value']
           metrics[date_str]['total_refunds_amt']['total_value'] += float(order['totalRefundedSet.shopMoney.amount'])
           metrics[date_str]['refunds_rate']['total_value'] = (metrics[date_str]['total_refunds']['total_value'] / metrics[date_str]['total_orders']['total_value']) * 100
           metrics[date_str]['total_shipping_refunds']['total_value'] += float(order['totalRefundedShippingSet.shopMoney.amount'])
           metrics[date_str]['net_sales']['total_value'] = metrics[date_str]['total_sales']['total_value'] - (metrics[date_str]['total_discounts_amt']['total_value'] + metrics[date_str]['total_refunds_amt']['total_value'])
           metrics[date_str]['total_tax']['total_value'] += float(order['totalTaxSet.shopMoney.amount'])
           metrics[date_str]['total_tip_received']['total_value'] += float(order['totalTipReceivedSet.shopMoney.amount'])
           metrics[date_str]["units_per_transaction"]['total_value'] = metrics[date_str]['total_units']['total_value'] / metrics[date_str]['total_orders']['total_value']
           metrics[date_str]["total_shipping_charges"]['total_value'] += float(order['totalShippingPriceSet.shopMoney.amount'])
           metrics[date_str]["average_shipping_charges"]['total_value'] = metrics[date_str]["total_shipping_charges"]['total_value'] / metrics[date_str]['total_orders']['total_value']
           metrics[date_str]["average_order_value"]['total_value'] = metrics[date_str]["total_subtotal"]['total_value'] / metrics[date_str]['total_orders']['total_value']
           if metrics[date_str]['total_shipping_charges']['total_value'] != 0:
            metrics[date_str]["average_shipping_discount"]['total_value'] = metrics[date_str]["total_shipping_discounts_amt"]['total_value'] / metrics[date_str]['total_shipping_charges']['total_value']
           else:
            metrics[date_str]["average_shipping_discount"]['total_value'] = 0
           metrics[date_str]["conversion_rate"]['total_value'] = metrics[date_str]['total_orders']['total_value'] / total_visitors
           metrics[date_str]["revenue_per_visitor"]['total_value'] = metrics[date_str]['total_sales']['total_value'] / total_visitors
           
           if int(order['customer.numberOfOrders']) == 1:
            metrics[date_str]["new_customers"]['total_value'] += 1

           if int(order['customer.numberOfOrders']) > 1:
             metrics[date_str]["repeat_customers"]['total_value'] += 1

           customer_id = order['customer.id'].split('/')
           
           customer_data.append(
              {
                customer_id[-1]: {
                  'amountSpent': float(order['customer.amountSpent.amount']),
                  'noOfOrders': int(order['customer.numberOfOrders']),
                }
              }
            )
        
        if abandoned_checkout_data_transformed[date_str]['count'] != 0:
           metrics[date_str]["cart_abandonment_rate"]['total_value'] = (abandoned_checkout_data_transformed[date_str]['count'] / (abandoned_checkout_data_transformed[date_str]['count'] + orders_data_transformed[date_str]['count'])) * 100
           

        for dim in dimensions:
            for order in orders_data_transformed[date_str]['data']:
              dict_key = dim.replace('.', '_') + "_wise"
              if dim in order:
                dim_value = order[dim]
                metrics[date_str]["total_orders"]['data'][dict_key][dim_value] += 1
                metrics[date_str]["total_units"]['data'][dict_key][dim_value] += order['subtotalLineItemsQuantity']
                metrics[date_str]["total_sales"]["data"][dict_key][dim_value] += float(order['totalPriceSet.shopMoney.amount'])
                if (order['displayFinancialStatus'] == 'REFUNDED'):
                  metrics[date_str]["total_refunds"]["data"][dict_key][dim_value] += 1
                metrics[date_str]["total_discounts_amt"]["data"][dict_key][dim_value] += float(order['totalDiscountsSet.shopMoney.amount'])
                
                if len(order['shippingLine.discountAllocations']) > 0:
                  metrics[date_str]["orders_with_shipping_discount"]["data"][dict_key][dim_value] += 1
                  for shipping_discount in order['shippingLine.discountAllocations']:
                    metrics[date_str]["total_shipping_discounts_amt"]["data"][dict_key][dim_value] += float(shipping_discount['allocatedAmountSet']['shopMoney']['amount'])

                metrics[date_str]["total_subtotal"]["data"][dict_key][dim_value] += float(order['subtotalPriceSet.shopMoney.amount'])
                metrics[date_str]["average_discounts"]["data"][dict_key][dim_value] = (metrics[date_str]["total_discounts_amt"]["data"][dict_key][dim_value] / metrics[date_str]["total_subtotal"]["data"][dict_key][dim_value]) * 100 
                metrics[date_str]["average_discounts_amt"]["data"][dict_key][dim_value] = metrics[date_str]["total_discounts_amt"]["data"][dict_key][dim_value] / metrics[date_str]["total_subtotal"]["data"][dict_key][dim_value]
                metrics[date_str]["total_refunds_amt"]["data"][dict_key][dim_value] += float(order['totalRefundedSet.shopMoney.amount'])
                metrics[date_str]["refunds_rate"]["data"][dict_key][dim_value] = (metrics[date_str]["total_refunds"]["data"][dict_key][dim_value] / metrics[date_str]["total_orders"]["data"][dict_key][dim_value]) * 100
                metrics[date_str]["total_shipping_refunds"]["data"][dict_key][dim_value] += float(order['totalRefundedShippingSet.shopMoney.amount'])
                metrics[date_str]["total_tax"]["data"][dict_key][dim_value] += float(order['totalTaxSet.shopMoney.amount'])
                metrics[date_str]["total_tip_received"]["data"][dict_key][dim_value] += float(order['totalTipReceivedSet.shopMoney.amount'])
                metrics[date_str]["net_sales"]["data"][dict_key][dim_value] += metrics[date_str]["total_sales"]["data"][dict_key][dim_value] - (metrics[date_str]["total_discounts_amt"]["data"][dict_key][dim_value] + metrics[date_str]["total_refunds_amt"]["data"][dict_key][dim_value])
                metrics[date_str]["units_per_transaction"]["data"][dict_key][dim_value] = metrics[date_str]["total_units"]['data'][dict_key][dim_value] / metrics[date_str]["total_orders"]['data'][dict_key][dim_value]
                metrics[date_str]["total_shipping_charges"]["data"][dict_key][dim_value] += float(order['totalShippingPriceSet.shopMoney.amount'])
                metrics[date_str]["average_shipping_charges"]["data"][dict_key][dim_value] = metrics[date_str]["total_shipping_charges"]["data"][dict_key][dim_value] / metrics[date_str]["total_orders"]['data'][dict_key][dim_value]
                metrics[date_str]["average_order_value"]["data"][dict_key][dim_value] = metrics[date_str]["total_subtotal"]["data"][dict_key][dim_value] / metrics[date_str]["total_orders"]['data'][dict_key][dim_value]
                if metrics[date_str]["total_shipping_charges"]['data'][dict_key][dim_value] != 0:
                  metrics[date_str]["average_shipping_discount"]["data"][dict_key][dim_value] = metrics[date_str]["total_shipping_discounts_amt"]["data"][dict_key][dim_value] / metrics[date_str]["total_shipping_charges"]['data'][dict_key][dim_value]
                else:
                  metrics[date_str]["average_shipping_discount"]["data"][dict_key][dim_value] = 0
                metrics[date_str]["conversion_rate"]["data"][dict_key][dim_value] = metrics[date_str]["total_orders"]['data'][dict_key][dim_value] / total_visitors
                metrics[date_str]["revenue_per_visitor"]["data"][dict_key][dim_value] = metrics[date_str]["total_sales"]['data'][dict_key][dim_value] / total_visitors
                
                if int(order['customer.numberOfOrders']) == 1:
                  metrics[date_str]["new_customers"]["data"][dict_key][dim_value] += 1

                if int(order['customer.numberOfOrders']) > 1:
                  metrics[date_str]["repeat_customers"]["data"][dict_key][dim_value] += 1
              else:
                dim_value = 'null'
  
  print("customer_data: ", customer_data)

  with open('order__graphQl_metrics.json', 'w') as file:
        json.dump(metrics, file, indent=4)

  return metrics

def init_dimension_data(dimensions, metrics, date_str, order):
    for dim in dimensions:
      dict_key = dim.replace('.', '_') + "_wise"
      if dim in order:
        dim_value = order[dim]
        metrics[date_str]["total_orders"]['data'][dict_key][dim_value] = 0
        metrics[date_str]['total_units']['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_sales"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_subtotal"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_discounts_amt"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_shipping_discounts_amt"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["average_discounts"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["average_discounts_amt"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["orders_with_shipping_discount"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_refunds"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["refunds_rate"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_refunds_amt"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_shipping_refunds"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["net_sales"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_tax"]["data"][dict_key][dim_value] = 0
        metrics[date_str]["total_tip_received"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["units_per_transaction"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["total_shipping_charges"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["average_shipping_charges"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["average_order_value"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["average_shipping_discount"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["conversion_rate"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["new_customers"]['data'][dict_key][dim_value] = 0
        metrics[date_str]["repeat_customers"]['data'][dict_key][dim_value] = 0
      else:
        dim_value = 'null'
    
    return metrics

def create_dimension_data(dimensions):
    dimension_data = {}
    for dim in dimensions:
        key = dim.replace('.', '_') + "_wise"
        dimension_data[key] = {}
    return dimension_data

def transform_datewise(data, group_by='date', source=any):
    date_format = "%Y-%m-%dT%H:%M:%S%z"
    strftime_format = '%Y-%m-%d %H' if group_by == 'date_hour' else '%Y-%m-%d'

    datewise_data = defaultdict(lambda: {"count": 0, "data": []})

    for item in data:
      df_item = pd.json_normalize(item)
      df_item = df_item.replace({np.nan: None})
      json_item = df_item.to_dict(orient='records')

      if (source == 'GraphQLAPI'):
        date = datetime.strptime(json_item[0]['createdAt'], date_format)
      elif ((source == 'RestAPI')):
         date = datetime.strptime(json_item[0]['created_at'], date_format)
      else:
        date = datetime.strptime(json_item[0]['createdAt'], date_format)
         
      formatted_date = date.strftime(strftime_format)

      datewise_data[formatted_date]['count'] += 1
      datewise_data[formatted_date]['data'].append(json_item[0]) 
   
    return datewise_data

getOrdersMetrics("2024-04-01", "2024-04-20", ['cancelReason', 'confirmed', 'currencyCode', 'customerAcceptsMarketing', 'displayFinancialStatus',
'displayFulfillmentStatus', 'returnStatus', 'billingAddress.city', 'billingAddress.country', 'billingAddress.province', 
'billingAddress.zip', 'channelInformation.app.title', 
'channelInformation.channelDefinition.handle', "customerJourneySummary.lastVisit.landingPage",
'customerJourneySummary.lastVisit.referrerUrl',
'customerJourneySummary.lastVisit.source',
'customerJourneySummary.lastVisit.sourceType',
'customerJourneySummary.lastVisit.referralCode',
'customerJourneySummary.lastVisit.marketingEvent',
'customerJourneySummary.lastVisit.utmParameters',
'customerJourneySummary.lastVisit.utmParameters.campaign',
'customerJourneySummary.lastVisit.utmParameters.content',
'customerJourneySummary.lastVisit.utmParameters.medium',
'customerJourneySummary.lastVisit.utmParameters.source',
'customerJourneySummary.lastVisit.utmParameters.term'
])
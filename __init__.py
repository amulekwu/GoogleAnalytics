import json
import pandas as pd
from datetime import date, timedelta, datetime
import os, logging
import time
import random
from urllib.error import HTTPError
from googleapiclient.discovery import build
# OR
# from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.errors import HttpError
import socket
import math

from azure.storage.blob import BlobClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

class SampledDataError(Exception):
    """Stop the program if query returns data that is sampled
    """
    pass


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
# KEY_FILE_LOCATION = '<REPLACE_WITH_JSON_FILE>'
# VIEW_ID = '<REPLACE_WITH_VIEW_ID>'

def initialize_analyticsreporting():

    # Create a Credentials object from the service account's credentials and the scopes your application needs access to.
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(GoogleAnalyticsKey, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def load_dimensions_and_metrics(file_path, datatype):
    """Load dimensions and metrics based on selected categories

    Args:
        file_path: where the AnalyticsReporting2_dimensions_and_metrics.json file is located
        datatype: category name of which dimensions and metrics to explore
    """
    file = open(file_path)
    dimensions_and_metrics = json.load(file)
    dimensions = dimensions_and_metrics[datatype]["dimensions"]
    metrics = dimensions_and_metrics[datatype]["metrics"]
    return dimensions, metrics

def get_report(analytics, startDate, endDate, dimensions, metrics, nextPageToken):

    return analytics.reports().batchGet(
    body={
        "reportRequests": [
            {
                "viewId": VIEW_ID,
                "dateRanges": [
                    {
                        "startDate": startDate,
                        "endDate": endDate
                    }
                ],
                "dimensions": [dimensions],
                "metrics": [metrics],
                "samplingLevel": "LARGE",
                "pageSize": 100000,
                "pageToken": nextPageToken
                }
            ]
    }
    ).execute()    

def get_ga_data(response):
    """Parses the Analytics Reporting API V4 response and turn the GA data into a report_ToList.
    
    Args:
        response: An Analytics Reporting API V4 response.
    """
    report_ToList = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            # create dicts for each row
            dicts = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            # fill dicts with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimensionHeaders, dimensions):
                dicts[header] = dimension

            for i, values in enumerate(dateRangeValues):
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    if ',' in value or '.' in value:
                        dicts[metricHeader.get('name')] = float(value)
                    else:
                        dicts[metricHeader.get('name')] = int(value)

            report_ToList.append(dicts)
    return report_ToList

def get_response(analytics, response, startDate, endDate, dimensions, metrics):
    """Parses the Analytics Reporting API V4 response after parse the nextPageToken and turn the whole GA data result into a DataFrame.
  
    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        response: An Analytics Reporting API V4 response.
        startDate: string
                   -> Returns The inclusive startDate for the query in the format YYYY-MM-DD.
                      Cannot be after endDate.
                      The format NdaysAgo, yesterday, or today is also accepted,
                      and in that case, the date is inferred based on the property's reporting time zone.
        endDate: string
                 -> Returns The inclusive endDate for the query in the format YYYY-MM-DD.
                    Cannot be before startDate.
                    The format NdaysAgo, yesterday, or today is also accepted,
                    and in that case, the date is inferred based on the property's reporting time zone.
        dimensions: [{object(Dimension)}] i.e., [{'name'': string}]
                    -> The dimensions requested. Requests can have a total of 9 dimensions.
        metrics: [{object(Metric)}] i.e., [{"expression": string}]
                 -> The metrics requested. Requests must specify at least one metric. Requests can have a total of 10 metrics.
  
    Output Parameters:
        nextPageToken: (1) string
                       -> A continuation token to get the next page of the results.
                          Adding this to the request will return the rows after the pageToken.
                          The pageToken should be the value returned in the nextPageToken parameter in the response to the reports.batchGet request.
                       (2) report_ToList
                       -> A report_ToList of all continuation token(s) to pass in the get_GAdata function get the whole result.
        completeData: report_ToList
                      -> A report_ToList of whole GA data
  
    Returns:
        df: whole GA data in a DataFrame
    """
    report0 = response.get('reports', [])[0]
    report_data = report0.get('data', {})
    row_count = report_data.get('rowCount')
    report_ToList = get_ga_data(response=response)
    row_remaining = row_count - len(report_ToList)
    print('Date Range: {} to {}. Row Count: {}.'.format(startDate, endDate, row_count))

    if report_data.get('samplesReadCounts', []) or report_data.get('samplingSpaceSizes', []):
        print('{} to {} contains sampled Data'.format(startDate, endDate))
        raise SampledDataError

    # pagination
    nextPageToken = report0.get('nextPageToken')

    offset_list = []
    offset = 0
    if row_count > 100000 and nextPageToken is not None:
        print(
            'Exceeded pagination limit. Missing Length of {} rows after first request.\n{} more request(s) passed.'.format(
                row_remaining, math.ceil(row_remaining / 100000)
            )
        )
        for i in range(math.ceil(row_count / 100000)):
            offset_list.append(offset)
            offset += 100000
    else:
        print('Finalized Segment.')
        offset_list.append(offset)

    nextPageToken_ToList = [str(i) for i in offset_list]

    completeData = []
    for token in nextPageToken_ToList:
        response = get_report(analytics=analytics, startDate=startDate, endDate=endDate, dimensions=dimensions,
                              metrics=metrics, nextPageToken=token)
        report_ToList = get_ga_data(response=response)
        completeData += report_ToList

    df = pd.DataFrame(completeData)
    # remove 'ga:' in all columnHeader
    df.columns = df.columns.str.replace('ga:', '') 
    return df

def save_finalized_report(file_path, startDate, endDate):
    """Queries through a list of categories

    Args:
        file_path: where the AnalyticsReporting2_dimensions_and_metrics.json file is located
        startDate: string
                   -> Returns The inclusive startDate for the query in the format YYYY-MM-DD.
                   Cannot be after endDate.
                   The format NdaysAgo, yesterday, or today is also accepted,
                   and in that case, the date is inferred based on the property's reporting time zone.
        endDate: string
                 -> Returns The inclusive endDate for the query in the format YYYY-MM-DD.
                 Cannot be before startDate.
                 The format NdaysAgo, yesterday, or today is also accepted,
                 and in that case, the date is inferred based on the property's reporting time zone.
    
    Returns:
        categorized dataframes: it should match with the lists that pass in.
    """
    # keep the lists up to date
    lists = []

    dimensions, metrics, df = {}, {}, {}
    for i in lists:
        dimensions[i], metrics[i] = load_dimensions_and_metrics(file_path=file_path, datatype=i)
        df[i] = pd.DataFrame(get_dataframes(startDate=startDate, endDate=endDate, dimensions=dimensions[i], metrics=metrics[i]))
        # print(dimensions[i], metrics[i], sep="\n")

        # setup for running on azure function apps
        data_file_name = '/tmp/{0}_{1}.csv'.format(i,endDate)
        df[i].to_csv('/tmp/{0}_{1}.csv'.format(i,endDate))
        upload_files_to_dataLake(data_file_name, connectionString, containerName)

def get_dataframes(startDate, endDate, dimensions, metrics):
    """Wrapper to request Google Analytics data with exponential backoff.
    
    The makeRequest method accepts the analytics service object, makes API requests and returns the response. 
    If any error occurs, the makeRequest method is retried using exponential backoff.

    Args:
        startDate: string
                   -> Returns The inclusive startDate for the query in the format YYYY-MM-DD.
                      Cannot be after endDate.
                      The format NdaysAgo, yesterday, or today is also accepted,
                      and in that case, the date is inferred based on the property's reporting time zone.
        endDate: string
                 -> Returns The inclusive endDate for the query in the format YYYY-MM-DD.
                    Cannot be before startDate.
                    The format NdaysAgo, yesterday, or today is also accepted,
                    and in that case, the date is inferred based on the property's reporting time zone.
        dimensions: [{object(Dimension)}] i.e., [{'name'': string}]
                    -> The dimensions requested. Requests can have a total of 9 dimensions.
        metrics: [{object(Metric)}] i.e., [{"expression": string}]
                 -> The metrics requested. Requests must specify at least one metric. Requests can have a total of 10 metrics.
    returns:
        df: whole GA data in a DataFrame
    """
    for n in range(0, 5): 
        try: 
            analytics = initialize_analyticsreporting()
            response = get_report(analytics=analytics, startDate=startDate, endDate=endDate, dimensions=dimensions, 
                                  metrics=metrics, nextPageToken='0')
            df = get_response(analytics=analytics, response=response, startDate=startDate, endDate=endDate, 
                              dimensions=dimensions, metrics=metrics)
            return df
        
        except HTTPError as error:
            if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded', 'internalServerError', 'backendError']:
                time.sleep((2**n) + random.random())
                print("There has been an error, the request never succeeded.")
            else:
                break

def main():
    """Parses the desired startDate and endDate to get the finalized reports in DataFrame structures.

    Parameters:
        file_path: where the AnalyticsReporting2_dimensions_and_metrics.json file is located
        startDate: string
                   -> Returns The inclusive startDate for the query in the format YYYY-MM-DD.
                   Cannot be after endDate.
                   The format NdaysAgo, yesterday, or today is also accepted,
                   and in that case, the date is inferred based on the property's reporting time zone.
        endDate: string
                 -> Returns The inclusive endDate for the query in the format YYYY-MM-DD.
                 Cannot be before startDate.
                 The format NdaysAgo, yesterday, or today is also accepted,
                 and in that case, the date is inferred based on the property's reporting time zone.
    """
    
    #
    # self-determined date ranges
    today = date.today()
    LatestDay = today - timedelta(1) #get yesterday's date to get latest data
    LatestDate = LatestDay.strftime("%Y-%m-%d")
    startDate, endDate = LatestDate, LatestDate
    save_finalized_report(file_path=file_path, startDate=startDate, endDate=endDate)
#    the output dataframe names shoule match the lists in Finalized_report


    # #Loop the date to get histroy data
    #end = date.today()
    # end = "2020-04-26"
    # end_DateFormat = datetime.strptime(end, "%Y-%m-%d")
    # date_range = 7
    # num_of_ranges = 340
    # for i in range(0,num_of_ranges):
    #     #self-determined date ranges
    #     endDate = (end_DateFormat - timedelta(i * date_range)).strftime("%Y-%m-%d")
    #     startDate = (end_DateFormat - timedelta((i+1) * date_range)).strftime("%Y-%m-%d")
    #     logging.info(startDate, endDate)
    #     save_finalized_report(file_path=file_path, startDate=startDate, endDate=endDate)
    #     time.sleep(3) #wait time to avoid google error 503

if __name__ == '__main__':
    main()
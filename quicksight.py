import os
import json
import boto3
import logging
import argparse
import hashlib
import botocore
import time

from pprint import pprint





#############################################################################
#  Utils
#############################################################################

def get_config(parser):
    # FIXEM replace with fire (?)
    subparsers = parser.add_subparsers()

    # Copy Dashboard Sub command
    ####################################
    parser_copy  = subparsers.add_parser('copy')
    parser_copy_sub = parser_copy.add_subparsers()

    parser_copy_dashboard = parser_copy_sub.add_parser('dashboard')
    parser_copy_dashboard.add_argument("--src-profile", action="store", dest="myAWSSrcProfile", required=True,
                        help="Your AWS source user Profile ")
    parser_copy_dashboard.add_argument("--dst-profile", action="store", dest="myAWSDstProfile", required=True,
                        help="Your AWS destination user Profile ")
    parser_copy_dashboard.add_argument("--src-dashboard-name", action="store", dest="mySrcDashboard",default="",
                        help="Copy command [dashboardname:version] ")
    parser_copy_dashboard.add_argument("--data-set-name", action="store", dest="myDstDataSetName",default="",
                        help="Destination data set name ")
    parser_copy_dashboard.add_argument("--comment", action="store", dest="myComment",default=None,
                        help="Comment for Template creation")
    parser_copy_dashboard.add_argument("--dst-dashaboard-name", action="store", dest="myDstDashboard",default=None,
                            help="Comment for Template creation")
    parser_copy_dashboard.add_argument("--dst-perm-name", action="store", dest="myDstGroupName",default="",
                            help="Comment for Template creation")
    parser_copy_dashboard.set_defaults(func=copyDashboard)

    # ingest params by




    # List
    ####################################
    parser_list = subparsers.add_parser('list')
    parser_list_sub = parser_list.add_subparsers()
    #common vars
    parser_list_common = argparse.ArgumentParser(add_help=False)
    parser_list_common.add_argument("--profile", action="store", dest="myAWSProfile", required=True,
                                    help="Your AWS  user Profile ")

    parser_list_dashboard = parser_list_sub.add_parser('dashboard', parents=[parser_list_common])
    parser_list_dashboard.set_defaults(func=listDashboards)

    parser_list_dataset = parser_list_sub.add_parser('dataset', parents=[parser_list_common])
    parser_list_dataset.set_defaults(func=listDataSets)

    parser_list_template = parser_list_sub.add_parser('template', parents=[parser_list_common])
    parser_list_template.set_defaults(func=listTemplates)

    # Desc
    ####################################
    parser_desc = subparsers.add_parser('desc')
    parser_desc_sub = parser_desc.add_subparsers()

    parser_desc_common = argparse.ArgumentParser(add_help=False)
    parser_desc_common.add_argument("--profile", action="store", dest="myAWSProfile", required=True,
                            help="Your AWS  user Profile ")
    parser_desc_common.add_argument("-id", action="store", dest="myItemId",default="",
                            help="ID ")

    parser_desc_dashboard = parser_desc_sub.add_parser('dashboard', parents=[parser_desc_common])
    parser_desc_dashboard.set_defaults(func=descDashboard)

    parser_desc_dataset = parser_desc_sub.add_parser('dataset', parents=[parser_desc_common])
    parser_desc_dataset.set_defaults(func=descDataSet)

    parser_desc_template = parser_desc_sub.add_parser('template', parents=[parser_desc_common])
    parser_desc_template.set_defaults(func=descTemplate)




    # Global params
    ####################################
    parser.add_argument("-v", "--verbosity", action="store",   dest="verbosity", default="INFO",
                        help="Debug level [ERROR|INFO|DEBUG]")


    args = parser.parse_args()
    return args




def init_logger( name, level, trace=False):
    #FIXME create config object
    log_level={'DEBUG':logging.DEBUG, 'INFO':logging.INFO, 'ERROR':logging.ERROR }
    logger = logging.getLogger(name)
    logger.setLevel(log_level[level])
    streamHandler = logging.StreamHandler()
    formatter     = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    return logger

def checkConfig(config,parser):
        pass


def check_status(response):
    if response['Status'] != 200:
        logger.error(f'Error : {response}')
        exit(1)


#############################################################################
#  Internal fucntions
#############################################################################
def copyDataSets():
        data_set_list = quicksight.list_data_sets(AwsAccountId=config.mySourceAccount)

        for data_set in data_set_list:
            data_set_details = quicksight.describe_data_sets(AwsAccountId=config.mySourceAccount,
                                                                DataSetId=data_set['DataSetId'])
            del data_set_details['Arn']
            del data_set_details['CreatedTime']
            del data_set_details['LastUpdatedTime']
            del data_set_details['ConsumedSpiceCapacityInBytes']

def getDashboardId(target,dashboardName):
    dashboards = quicksight[target].list_dashboards(AwsAccountId=target)
    #print(dashboards)

    for item in dashboards['DashboardSummaryList']:
        if item['Name'] == dashboardName:
            return item

    return None


def getDashboardById(dashboardId,target,VersionNumber=None):
    dashboard = quicksight[target].describe_dashboard(AwsAccountId=target,
                                              DashboardId=dashboardId,
                                              VersionNumber=VersionNumber)['Dashboard']
    logger.debug('Describe dashboard '+ str(dashboard))
    return dashboard

#cyclete over the dataset
def setTemplate(dashboard,target,VersionNumber=None):
    dashboard_name = dashboard['Name']
    template_name = dashboard['Name']+'-'+VersionNumber+'-template'
    template_id = hashlib.sha224(template_name.encode()).hexdigest()

    #FIXME switch to use ALIAS
    entry = None
    logger.info(f'Searching template {template_id}')
    try :
        entry = quicksight[target].describe_template( AwsAccountId=target,TemplateId=template_id)
        logger.info(f'Found template:  {template_id} ')
        logger.debug(f'Found template:  {entry} ')
    except botocore.exceptions.ClientError as error:
         if error.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.info(f'Template {template_id} Not Found ')


    template_dict = {'AwsAccountId':target,
                'TemplateId': template_id,
                'Name': dashboard['Name']+'-template',
                'SourceEntity':{
                    'SourceAnalysis': {
                        'Arn': dashboard['Version']['SourceEntityArn'],
                        'DataSetReferences': [
                            {
                                'DataSetPlaceholder': 'weldlog',
                                'DataSetArn': dashboard['Version']['DataSetArns'][0]
                            },
                         ]
                      },
                 },
                 'VersionDescription':VersionNumber
                }

    logger.debug(f'Template {template_dict}')

    if entry is None:
        entry = quicksight[target].create_template(**template_dict)
        logger.info(f'Template created with id {template_id} ')
    else :
        entry = quicksight[target].update_template(**template_dict)
        logger.info(f'Template updated with id {template_id}')

    #FIXME Check the Status and wait until resource is created


    return entry




def setDashboard(dashboard, template, dataset, target , dashboardDstName, dstGroupName, comment):
    item = None

    if dashboardDstName is None:
        dashboardDstName = dashboard['Name']

    dashboardDstId = hashlib.sha224(dashboardDstName.encode()).hexdigest()
    #dashboardDstId =  dashboard['DashboardId']+"-"+target

    try:
        item = getDashboardId(target,dashboardDstName)
        logger.info(f'Found dashboard in target account : {dashboardDstName}')
        logger.debug(f'Found Dashboard object : {item}')
    except botocore.exceptions.ClientError as error:
         if error.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.info(f'Dashboard {dashboardDstName} not Found')

    if comment is None:
       comment = "-"

    # ADD some specific permissions?
    entry =  {
                    "AwsAccountId": target ,
                    "DashboardId": dashboardDstId,
                    "Name": dashboardDstName,
                    "SourceEntity": {
                        "SourceTemplate": {
                            "DataSetReferences": [
                                {
                                    "DataSetPlaceholder": "weldlog",
                                    "DataSetArn": dataset['Arn']
                                }
                            ],
                            "Arn": template['Arn']
                        }
                    },
                    "VersionDescription": comment,
                    "DashboardPublishOptions": {
                        "AdHocFilteringOption": {
                            "AvailabilityStatus": "ENABLED"
                        },
                        "ExportToCSVOption": {
                            "AvailabilityStatus": "ENABLED"
                        },
                        "SheetControlsOption": {
                            "VisibilityState": "EXPANDED"
                        }
                    }
                }
 
    logger.debug(f'Dasboard entry : {entry} ')


    if item is None:
        logger.info(f'Dasboard created in target account : {dashboardDstName} ')
        entry = quicksight[target].create_dashboard(**entry)
    else:
        logger.info(f'Dasboard updated in target account : {dashboardDstName} ')
        entry = quicksight[target].update_dashboard(**entry)
        logger.info(f'Dasboard updated in target account : {dashboardDstName} ')

    logger.debug(f'Dashboard Creation result : {entry}')

    #FIXME check the response
    dstVersion = int(entry['VersionArn'].split('/')[-1])

    #Check status
    while True:
        item = getDashboardById(dashboardDstId,target,dstVersion)
        logger.info(f"Status : {item['Version']['Status']} ")
        logger.debug(f'Status : {item} ')
        if 'Errors' in item['Version'] and len(item['Version']['Errors']) > 0:
            logger.error(item['Version']['Errors'])
            exit(1)

        if item['Version']['Status'] == 'CREATION_SUCCESSFUL':
            break
        time.sleep(10)

    response = quicksight[target].update_dashboard_published_version(AwsAccountId=target ,
                                                         DashboardId=dashboardDstId,
                                                         VersionNumber=dstVersion)
    check_status(response)
    logger.info(f'Dasboard published in target account : {dashboardDstName} with version {dstVersion} ')

    grpPerm = f"user/default/PA_DEVELOPER/{dstGroupName}"
    #update permission
    perm =  [
                    {
                        "Principal": f"arn:aws:quicksight:eu-west-1:{target}:{grpPerm}",
                        "Actions": [
                            "quicksight:DescribeDashboard",
                            "quicksight:ListDashboardVersions",
                            "quicksight:UpdateDashboardPermissions",
                            "quicksight:QueryDashboard",
                            "quicksight:UpdateDashboard",
                            "quicksight:DeleteDashboard",
                            "quicksight:DescribeDashboardPermissions",
                            "quicksight:UpdateDashboardPublishedVersion"
                        ]
                    }
                ]

    logger.debug(f'Set Permission : {perm}')
    response = quicksight[target].update_dashboard_permissions( AwsAccountId=target,
                                                        DashboardId=dashboardDstId,
                                                        GrantPermissions=perm)
    # CHECK Result
    check_status(response)
    logger.info(f'Dashboard permission grant to: {grpPerm}')



def getDataSetId(myDstDataSetName,target):
    data_set = quicksight[target].list_data_sets(AwsAccountId=target)

    for item in data_set['DataSetSummaries']:
        if item['Name'] == myDstDataSetName:
            logger.info(f"Find Data set in target account : {item['Arn']}")
            logger.debug(f'Find Data set in target account : {item}')
            return item

    return None


def setTemplatePerm(template, dst, src):

    if dst == src:
        logger.debug(f'Set permission skipped, source {src} and destination {dst} account are the same ')
        return

    entry = {'AwsAccountId':src,
             'TemplateId':template['TemplateId'],
             'GrantPermissions':[
                {
                    'Principal': f'arn:aws:iam::{dst}:root',
                    #'Principal': f'arn:aws:iam::099769128691:root',
                    'Actions': [
                        "quicksight:DescribeTemplate",
                        "quicksight:UpdateTemplatePermissions"
                    ]
                 }
               ]
              }

    response = quicksight[src].update_template_permissions(**entry)


#############################################################################
#  Commands
#############################################################################
def copyDashboard(config):
    global logger, quicksight

    #Open Conenctions
    logger.info(f'**Dashboard creation started ')
    session = boto3.session.Session(profile_name=config.myAWSSrcProfile)
    src = config.myAWSSrcProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')
    session = boto3.session.Session(profile_name=config.myAWSDstProfile)
    dst = config.myAWSDstProfile.split('_')[0]
    quicksight[dst] = session.client('quicksight')

    dashboardSrcName, dashboardVersion = config.mySrcDashboard.split(':')

    # Search Dashboard ID
    dashboardId = getDashboardId(src, dashboardSrcName)['DashboardId']

    logger.info(f'Dashboard src ID : {dashboardId}')

    # Fetch the Dashboard Description
    dashboard = getDashboardById(dashboardId, src, int(dashboardVersion) )

    #Create / Update template
    template = setTemplate(dashboard,src,dashboardVersion)

    #set permissions to tempalte
    setTemplatePerm(template, dst, src)

    #Fetch dataset ARN
    dataSet = getDataSetId(config.myDstDataSetName,dst)
    #dataSet = {'Arn': config.myDstDataSetName}

    #Create / Update Dashboard
    setDashboard(dashboard, template, dataSet, dst , config.myDstDashboard , config.myDstGroupName, config.myComment)

    #FIXME movethe permission out of the dashboard Creation
    #setDashboardPerm()

    logger.info(f'**Dashboard creation Finished')


def listDashboards(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')
    dashboards = quicksight[src].list_dashboards(AwsAccountId=src)

    #FIXME create a generic function for print
    print(f'Dashboard List of the account {src}')
    print('-------------------------------------------------------------------')
    for item in dashboards['DashboardSummaryList']:
        print(f'{item["DashboardId"]}|{item["Name"]}|{item["PublishedVersionNumber"]}')

def listTemplates(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')
    templates = quicksight[src].list_templates(AwsAccountId=src)

    print(f'Templates List of the account {src}')
    print('-------------------------------------------------------------------')
    for item in templates ['TemplateSummaryList']:
        print(f'{item["TemplateId"]}|{item["Name"]}|{item["LatestVersionNumber"]}')

def listDataSets(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')
    dataSets = quicksight[src].list_data_sets(AwsAccountId=src)
    print(dataSets)
    print(f'DataSets List of the account {src}')
    print('-------------------------------------------------------------------')
    for item in dataSets['DataSetSummaries']:
        print(f'{item["DataSetId"]}|{item["Name"]}')


def descDashboard(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')

    dashboard = quicksight[src].describe_dashboard(AwsAccountId=src,
                                                   DashboardId=config.myItemId)
                                                   #VersionNumber=int(config.myVersion))

    pprint(dashboard)

def descDataSet(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')

    dataset = quicksight[src].describe_data_set(AwsAccountId=src,
                                                   DataSetId=config.myItemId,
                                                   )
    pprint(dataset)


def descTemplate(config):
    quicksight = dict()
    session = boto3.session.Session(profile_name=config.myAWSProfile)
    src = config.myAWSProfile.split('_')[0]
    quicksight[src] = session.client('quicksight')

    template = quicksight[src].describe_template(AwsAccountId=src,
                                                   TemplateId=config.myItemId,
                                                   )
    pprint(template)


##############################################################################
# MAIN
##############################################################################
quicksight = dict()
config = None
parser = argparse.ArgumentParser()

config = get_config(parser)
    #checkConfig(config,parser)

logger = init_logger("QuickSight", config.verbosity)

#Execute command
config.func(config)
#main()

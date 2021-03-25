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

    parser_copy_common = argparse.ArgumentParser(add_help=False)
    parser_copy_common.add_argument("--src-profile", action="store", dest="myAWSSrcProfile", required=True,
                        help="Your AWS source user Profile ")
    parser_copy_common.add_argument("--dst-profile", action="store", dest="myAWSDstProfile", required=True,
                        help="Your AWS destination user Profile ")
    parser_copy_common.add_argument("--src-name", action="store", dest="mySrcItemName",default="",
                        help="Copy command [iteam:version] ")
    parser_copy_common.add_argument("--data-set-name", action="store", dest="myDstDataSetName",default="",
                        help="Destination data set name ")
    parser_copy_common.add_argument("--dst-name", action="store", dest="myDstItemName",default=None,
                            help="Destination name")
    parser_copy_common.add_argument("--dst-perm-name", action="store", dest="myDstGroupName",default="",
                            help="Comment for Template creation")
    parser_copy_common.add_argument("--comment", action="store", dest="myComment",default="",
                            help="Comment for Template creation")

    parser_copy_sub = parser_copy.add_subparsers()
    parser_copy_dashboard = parser_copy_sub.add_parser('dashboard', parents=[parser_copy_common])

    parser_copy_dashboard.add_argument("--src-version", action="store", dest="mySrcItemVer",default=None,
                                help="Version [item:version] ")
    parser_copy_dashboard.set_defaults(func=copyItem,itemType='dashboard')

    parser_copy_analysis = parser_copy_sub.add_parser('analysis', parents=[parser_copy_common])
    parser_copy_analysis.add_argument("--src-version", action="store", dest="mySrcItemVer",default=None,
                                help="Version [item:version] ")
    parser_copy_analysis.set_defaults(func=copyItem,itemType='analysis')


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

    parser_list_analysis = parser_list_sub.add_parser('analysis', parents=[parser_list_common])
    parser_list_analysis.set_defaults(func=listAnalysis)

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

    parser_desc_analysis = parser_desc_sub.add_parser('analysis', parents=[parser_desc_common])
    parser_desc_analysis.set_defaults(func=descAnalysis)


    # Global params
    ####################################
    #FIXME move to parents
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

def getDashboardId(quicksight,awsAccountId,dashboardName):
    data = quicksight.list_dashboards(AwsAccountId=awsAccountId)
    #print(dashboards)

    for item in data['DashboardSummaryList']:
        if item['Name'] == dashboardName:
            return item

    return None


def getAnalysisId(quicksight, awsAccountId, analysisName):
    data = quicksight.list_analyses(AwsAccountId=awsAccountId)

    for item in data['AnalysisSummaryList']:
        if item['Name'] == analysisName:
            return item

    return None


def getDashboardById(quicksight, awsAccountId , dashboardId,VersionNumber=None):
    dashboard = quicksight.describe_dashboard(AwsAccountId=awsAccountId,
                                              DashboardId=dashboardId,
                                              VersionNumber=VersionNumber)['Dashboard']
    logger.debug('Describe dashboard '+ str(dashboard))
    return dashboard

def getAnalysisById(quicksight, awsAccountId , analisysId ):
    analysis = quicksight.describe_analysis(AwsAccountId=awsAccountId,
                                              AnalysisId=analisysId)['Analysis']
    logger.debug('Describe dashboard '+ str(analysis))
    return analysis

#cyclete over the dataset
def setTemplate(quicksight,awsAccountId,itemName,entityArn,dataSetArn,VersionNumber='0'):

    template_name = itemName+'-'+ VersionNumber+'-template'
    template_id = hashlib.sha224(template_name.encode()).hexdigest()

    #FIXME switch to use ALIAS
    entry = None
    logger.info(f'Searching template {template_id}')
    try :
        entry = quicksight.describe_template( AwsAccountId=awsAccountId,TemplateId=template_id)
        logger.info(f'Found template:  {template_id} ')
        logger.debug(f'Found template:  {entry} ')
    except botocore.exceptions.ClientError as error:
         if error.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.info(f'Template {template_id} Not Found ')


    template_dict = {   'AwsAccountId':awsAccountId,
                        'TemplateId': template_id,
                        'Name': itemName+'-template',
                        'SourceEntity':{
                            'SourceAnalysis': {
                                'Arn': entityArn,
                                #'Arn': dashboard['Version']['SourceEntityArn'],
                                'DataSetReferences': [
                                    {
                                        'DataSetPlaceholder': 'dataset_placeholder',
                                        'DataSetArn': dataSetArn
                                        #'DataSetArn': dashboard['Version']['DataSetArns'][0]
                                    }
                                 ]
                              },
                         },
                         'VersionDescription':VersionNumber
                    }

    logger.debug(f'Template {template_dict}')

    if entry is None:
        entry = quicksight.create_template(**template_dict)
        logger.info(f'Template created with id {template_id} ')
    else :
        entry = quicksight.update_template(**template_dict)
        logger.info(f'Template updated with id {template_id}')

    #FIXME Check the Status and wait until resource is created
    return entry



def setAnalysis(quicksight, analysisName, templateArn, datasetArn, awsAccount, dstGroupName, ItemVer, comment):
    item = None
    analysisId = hashlib.sha224(analysisName.encode()).hexdigest()

    try:
        item = getAnalysisId(quicksight, awsAccount, analysisName)
        logger.info(f'Found analysis  : {item}')
        logger.debug(f'Found alaysis object : {item}')
    except botocore.exceptions.ClientError as error:
         if error.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.info(f'Dashboard {analysisName} not Found')

    entry =  {
                    "AwsAccountId": awsAccount ,
                    "AnalysisId": analysisId,
                    "Name": analysisName,
                    "SourceEntity": {
                        "SourceTemplate": {
                            "DataSetReferences": [
                                {
                                    "DataSetPlaceholder": "dataset_placeholder",
                                    "DataSetArn": datasetArn
                                    #"DataSetArn": dataset['Arn']
                                }
                            ],
                            "Arn": templateArn
                            #"Arn": template['Arn']
                        }
                    }
                }

    logger.debug(f'analysis entry : {entry} ')


    if item is None:
        logger.info(f'analysis created in target account : {analysisName} ')
        response = quicksight.create_analysis(**entry)
    else:
        logger.info(f'analysis updated in target account : {analysisName} ')
        response = quicksight.update_analysis(**entry)


    logger.debug(f'Analysis  Creation result : {entry}')


    #Check status
    while True:

        item = getAnalysisById(quicksight, awsAccount , analysisId )

        logger.info(f"Status : {item['Status']} ")
        logger.debug(f'Status : {item} ')
        if 'Errors' in item and len(item['Errors']) > 0:
            logger.error(item['Errors'])
            exit(1)

        if item['Status'] == 'CREATION_SUCCESSFUL' or item['Status'] == 'UPDATE_SUCCESSFUL':
            break
        time.sleep(10)

    #update permission
    perm = getPermAnalisysTemplate(dstGroupName, awsAccount)

    logger.debug(f'Set Permission : {perm}')
    logger.info(f'Analysis  permission grant to: {dstGroupName}')
    response = quicksight.update_analysis_permissions( AwsAccountId=awsAccount,
                                                        AnalysisId=analysisId,
                                                        GrantPermissions=perm)
    # CHECK Result
    check_status(response)


def setDashboard(quicksight, dashboardName, templateArn, datasetArn, awsAccount , dstGroupName, dashboardVer, comment):
    item = None
    dashboardId = hashlib.sha224(dashboardName.encode()).hexdigest()
    #dashboardDstId =  dashboard['DashboardId']+"-"+target

    try:
        item = getDashboardId(quicksight, awsAccount, dashboardName)
        logger.info(f'Found dashboard in target account : {dashboardName}')
        logger.debug(f'Found Dashboard object : {item}')
    except botocore.exceptions.ClientError as error:
         if error.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.info(f'Dashboard {dashboardName} not Found')


    # ADD some specific permissions?
    entry =  {
                    "AwsAccountId":awsAccount ,
                    "DashboardId": dashboardId,
                    "Name": dashboardName,
                    "SourceEntity": {
                        "SourceTemplate": {
                            "DataSetReferences": [
                                {
                                    "DataSetPlaceholder": "dataset_placeholder",
                                    "DataSetArn": datasetArn
                                    #"DataSetArn": dataset['Arn']
                                }
                            ],
                            "Arn": templateArn
                            #"Arn": template['Arn']
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
        logger.info(f'Dasboard created in target account : {dashboardName} ')
        entry = quicksight.create_dashboard(**entry)
    else:
        logger.info(f'Dasboard updated in target account : {dashboardName} ')
        entry = quicksight.update_dashboard(**entry)
        logger.info(f'Dasboard updated in target account : {dashboardName} ')

    logger.debug(f'Dashboard Creation result : {entry}')

    #FIXME check the response
    dstVer = int(entry['VersionArn'].split('/')[-1])

    #Check status
    while True:
        item =  getDashboardById(quicksight, awsAccount , dashboardId, int(dashboardVer) )
        logger.info(f"Status : {item['Version']['Status']} ")
        logger.debug(f'Status : {item} ')
        if 'Errors' in item['Version'] and len(item['Version']['Errors']) > 0:
            logger.error(item['Version']['Errors'])
            exit(1)

        if item['Version']['Status'] == 'CREATION_SUCCESSFUL' or item['Version']['Status'] == 'UPDATE_SUCCESSFUL':
            break
        time.sleep(10)

    response = quicksight.update_dashboard_published_version(AwsAccountId=awsAccount ,
                                                         DashboardId=dashboardId,
                                                         VersionNumber=int(dashboardVer))
    check_status(response)
    logger.info(f'Dasboard published in target account : {dashboardName} with version {dashboardVer} ')

    #update permission
    perm = getPermTemplate(dstGroupName, awsAccount)

    logger.debug(f'Set Permission : {perm}')
    logger.info(f'Dashboard permission grant to: {dstGroupName}')
    response = quicksight.update_dashboard_permissions( AwsAccountId=awsAccount,
                                                        DashboardId=dashboardId,
                                                        GrantPermissions=perm)
    # CHECK Result
    check_status(response)




def getPermTemplate(dstGroupName, awsAccountId):
    grpPerm = f"user/default/PA_DEVELOPER/{dstGroupName}"
    #update permission
    perm =  [
                {
                    "Principal": f"arn:aws:quicksight:eu-west-1:{awsAccountId}:{grpPerm}",
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
                },
                {
                    "Principal": f"arn:aws:quicksight:eu-west-1:{awsAccountId}:namespace/default",
                    "Actions": [
                        "quicksight:DescribeDashboard",
                        "quicksight:ListDashboardVersions",
                        "quicksight:QueryDashboard"
                    ]
                }
            ]
    return perm


def getPermAnalisysTemplate(dstGroupName, awsAccountId):
    grpPerm = f"user/default/PA_DEVELOPER/{dstGroupName}"
    #update permission
    perm =  [
                {
                    "Principal": f"arn:aws:quicksight:eu-west-1:{awsAccountId}:{grpPerm}",
                    "Actions": [
                        "quicksight:RestoreAnalysis",
                        "quicksight:UpdateAnalysisPermissions",
                        "quicksight:DeleteAnalysis",
                        "quicksight:QueryAnalysis",
                        "quicksight:DescribeAnalysisPermissions",
                        "quicksight:DescribeAnalysis",
                        "quicksight:UpdateAnalysis"
                    ]
                },
                {
                    "Principal": f"arn:aws:quicksight:eu-west-1:{awsAccountId}:namespace/default",
                    "Actions": [
                        "quicksight:DescribeAnalysis",
                        "quicksight:QueryAnalysis"
                    ]
                }
            ]
    return perm

def getDataSetId(quicksight, awsAccountId, myDstDataSetName):
    data_set = quicksight.list_data_sets(AwsAccountId=awsAccountId)

    for item in data_set['DataSetSummaries']:
        if item['Name'] == myDstDataSetName:
            logger.info(f"Find Data set in target account : {item['Arn']}")
            logger.debug(f'Find Data set in target account : {item}')
            return item

    return None


def setTemplatePerm(quicksight,awsSrcAccount,awsDstAccount,templateId):

    if awsSrcAccount==awsDstAccount:
        logger.debug(f'Set permission skipped, source {awsSrcAccount} and destination {awsDstAccount} account are the same ')
        return

    entry = {'AwsAccountId':awsSrcAccount,
             'TemplateId':templateId,
             'GrantPermissions':[
                {
                    'Principal': f'arn:aws:iam::{awsDstAccount}:root',
                    #'Principal': f'arn:aws:iam::099769128691:root',
                    'Actions': [
                        "quicksight:DescribeTemplate",
                        "quicksight:UpdateTemplatePermissions"
                    ]
                 }
               ]
              }

    response = quicksight.update_template_permissions(**entry)


def getAwsAccount(profile):
    return profile.split('_')[0]

def getQskSession(myAWSProfile):
    session = boto3.session.Session(profile_name=myAWSProfile)
    return session.client('quicksight')

def printTable(title,data,fields):
    #FIXME moveto pretty table
    print(f'{title}')
    print('-------------------------------------------------------------------')
    for item in data:
        row = []
        for field in fields:
            row.append(f'{item[field]}\t')
        print(''.join(row))


#############################################################################
#  Commands
#############################################################################


def copyItem(config):
    global logger, quicksight

    #Open Conenctions
    logger.info(f'** {config.itemType} creation started ')
    awsSrcAccount = getAwsAccount(config.myAWSSrcProfile)
    quicksight[awsSrcAccount] = getQskSession(config.myAWSSrcProfile)
    awsDstAccount = getAwsAccount(config.myAWSDstProfile)
    quicksight[awsDstAccount] = getQskSession(config.myAWSDstProfile)

    if config.itemType == 'dashboard' :
        #Get Dashboard ID
        item = getDashboardId(quicksight[awsSrcAccount], awsSrcAccount,
                                        config.mySrcItemName)
        logger.info(f"Dashboard src ID : {item['DashboardId']}")
        #Get Dashboard description
        item = getDashboardById(quicksight[awsSrcAccount], awsSrcAccount,
                                        item['DashboardId'], int(config.mySrcItemVer) )

        dataSetSrcArn = item['Version']['DataSetArns'][0]
        entityArn  = item['Version']['SourceEntityArn']
    elif config.itemType == 'analysis':
        #Get Dashboard ID
        item = getAnalysisId(quicksight[awsSrcAccount], awsSrcAccount, config.mySrcItemName)
        logger.debug(f"Analysis  : {item}")
        item = getAnalysisById(quicksight[awsSrcAccount], awsSrcAccount,
                                        item['AnalysisId'])
        logger.debug(f"Analysis  : {item}")
        logger.info(f"Analysis src ID : {item['AnalysisId']}")
        dataSetSrcArn = item['DataSetArns'][0]
        entityArn  = item['Arn']
        config.mySrcItemVer = "1"



    #Create / Update template
    template = setTemplate(quicksight[awsSrcAccount],awsSrcAccount,config.mySrcItemName,
                                entityArn, dataSetSrcArn, config.mySrcItemVer)

    #set permissions to tempalte
    setTemplatePerm(quicksight[awsSrcAccount],awsSrcAccount,awsDstAccount,
                                template['TemplateId'])

    #Fetch dataset ARN
    dataSetDst = getDataSetId(quicksight[awsDstAccount], awsDstAccount,
                                config.myDstDataSetName)
    if dataSetDst is None:
        raise Exception(f'Destination dataset not found: {config.myDstDataSetName}')
    logger.info(f"Destination DataSet id : {dataSetDst['DataSetId']}")

    #Create / Update Item

    if config.myDstItemName is None:
        config.myDstItemName = config.mySrcItemName

    if config.myComment is None:
        config.myComment = '-'

    if config.itemType == 'dashboard' :
        setDashboard(quicksight[awsDstAccount], config.myDstItemName, template['Arn'], dataSetDst['Arn'], awsDstAccount,
                        config.myDstGroupName,config.mySrcItemVer,
                        config.myComment)
    elif config.itemType == 'analysis':
        setAnalysis(quicksight[awsDstAccount], config.myDstItemName, template['Arn'], dataSetDst['Arn'], awsDstAccount,
                        config.myDstGroupName, config.mySrcItemVer,
                        config.myComment)

    #FIXME movethe permission out of the dashboard Creation
    #setDashboardPerm()

    logger.info(f'**{config.itemType} creation Finished')



#
# LIST Section
###############################################################################
#FIXME build a decorator
def listDashboards(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    data = quicksight.list_dashboards(AwsAccountId=awsAccount)

    printTable(f'Dashboard List of the account {awsAccount}',
                                data['DashboardSummaryList'] ,
                                ["DashboardId","Name","PublishedVersionNumber"])

def listTemplates(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    data = quicksight.list_templates(AwsAccountId=awsAccount)

    printTable(f'Templates List of the account {awsAccount}',
                                data['TemplateSummaryList'] ,
                                ["TemplateId","Name","LatestVersionNumber"])

def listDataSets(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    data = quicksight.list_data_sets(AwsAccountId=awsAccount)

    printTable(f'DataSet List of the account {awsAccount}',
                                data['DataSetSummaries'] ,
                                ["DataSetId","Name"])


def listAnalysis(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    data = quicksight.list_analyses(AwsAccountId=awsAccount)


    printTable(f'Templates List of the account {awsAccount}',
                                    data['AnalysisSummaryList'] ,
                                    ["AnalysisId","Name","Status"])


#
# LIST Section
###############################################################################
def descDashboard(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    dashboard = quicksight.describe_dashboard(AwsAccountId=awsAccount,
                                                   DashboardId=config.myItemId)
                                                   #VersionNumber=int(config.myVersion))

    pprint(data)

def descDataSet(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    dataset = quicksight.describe_data_set(AwsAccountId=awsAccount,
                                                   DataSetId=config.myItemId,
                                                   )
    pprint(dataset)


def descTemplate(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    template = quicksight.describe_template(AwsAccountId=awsAccount ,
                                                   TemplateId=config.myItemId,
                                                   )
    pprint(template)

def descAnalysis(config):
    awsAccount = getAwsAccount(config.myAWSProfile)
    quicksight = getQskSession(config.myAWSProfile)
    template = quicksight.describe_analysis(AwsAccountId=awsAccount ,
                                                   AnalysisId=config.myItemId,
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

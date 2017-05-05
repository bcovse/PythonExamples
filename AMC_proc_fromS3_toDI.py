import json
import logging
import os.path
import urllib
import boto
import dateutil.parser
import requests
import xmltodict
from boto.s3.connection import S3Connection
from urllib2 import URLError, HTTPError
from urllib2 import urlopen

################################  Modify before use ######################################################
# Hostnames (for file locations or callback URLs) must not contain underscore characters or certain other
# punctuation or special characters per IETF specifications. See this Wikipedia article for more
# information. Errors will be returned if hostnames do not conform to the IETF standard.

logging_level = logging.DEBUG
##############################  AMC CONFIDENTIAL  ########################################################
aws_keyId = "ASK ME"
aws_sKeyId = "ASK ME"
aws_is_secure = True
aws_port = 443
aws_host = "s3.amazonaws.com"
aws_path = "bc-ingest.amcnetworks.com"
##########################################################################################################
client_id = "ASK ME"
client_secret = "ASK ME"
pub_id = "5049773606001"
##########################################################################################################
configfile = "jobs.json"
jobsUrl = ""  # ""https://" + aws_host + "/" + aws_path + "/" + jobsFile
xmlFile = "jobs.xml"
xmlUrl = ""  # "https://" + aws_host + "/" + aws_path + "/" + xmlFile
useXml = True
access_token_url = "https://oauth.brightcove.com/v3/access_token"
profiles_base_url = "http://ingestion.api.brightcove.com/v1/accounts/{pub_id}/profiles"
jobProfile = "high-resolution"
jobsData = []
xmlData = []
myFileList = []
conn = []
maxnumjobs = 20


##########################################################################################################

def get_access_token():
    access_token = None
    r = requests.post(access_token_url, params="grant_type=client_credentials", auth=(client_id, client_secret),
                      verify=False)
    logging.debug("get_access_token.r:  {s}".format(s=r.json()))
    if r.status_code == 200:
        access_token = r.json().get('access_token')
    return access_token


def create_video(cmsobjdata):
    access_token = get_access_token()
    headers = {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}
    try:
        url = "https://cms.api.brightcove.com/v1/accounts/{pubid}/videos/".format(pubid=pub_id)
    except URLError as e:
        logging.error("create_video: URL error.  Check internet connection")
        logging.error("create_video: {s}".format(s=e.reason))
        exit(1)

    except HTTPError as e:
        logging.error("create_video:  {s}".format(s=e.code))
        logging.error("create_video:  {s}".format(s=e.read))
        exit(1)

    data = json.dumps(cmsobjdata)
    r = requests.post(url, headers=headers, data=data)
    logging.debug("create_video.r:  {s}".format(s=r.json()))
    return r.json()


def submit_pbi(video_id):
    access_token = get_access_token()
    headers = {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}

    try:
        ingesturl = "https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos/{videoid}/ingest-requests".format(
            pubid=pub_id, videoid=video_id)
        logging.debug("submit_pbi.ingesturl:  {s}".format(s=ingesturl))

        #  https://s3.amazonaws.com/djfmynewtestbucket/AMC_TD_706_Highlights.mov
        masterurl = "https://{host}/{path}/{fname}".format(host=aws_host, path=aws_path,
                                                           fname=jobsData["publisher-upload-manifest"]["asset"][
                                                               "filename"])
        logging.debug("submit_pbi.masterurl:  {s}".format(s=masterurl))
        #  I have no clue why thiws does not work.  Using hack
        #  data = '''{"master": {"url":{murl}},"profile":{jprofile}}'''.format(murl=masterurl, jprofile=jobProfile)
        data = '''{"master": { "url": "''' + masterurl + '''" }, "profile": "''' + jobProfile + '''"}'''

        mdata = json.dumps(data)

        logging.debug("submit_pbi.data:  {s}".format(s=mdata))

        r = requests.post(ingesturl, headers=headers, data=data)
        logging.debug("submit_pbi.r: {s}".format(s=r.json()))

    except URLError as e:
        logging.error("submit_pbi: URL error.  Check internet connection")
        logging.error("submit_pbi: {s}".format(s=e.reason))
        exit(1)

    except HTTPError as e:
        logging.error("submit_pbi:  {s}".format(s=e.code))
        logging.error("submit_pbi:  {s}".format(s=e.read))
        exit(1)

    return r.json()


def return_video_metadata_by_name(video_name):
    access_token = get_access_token()
    headers = {'Authorization': 'Bearer ' + access_token, "Content-Type": "application/json"}
    url = "https://ingest.api.brightcove.com/v1/accounts/{pubid}/videos?q=name:{namestr}".format(pubid=pub_id,
                                                                                                 namestr=video_name)
    r = requests.get(url, headers=headers)
    logging.debug("return_video_metadata_by_name.r:  {s}".format(s=r.json()))
    return r.json()


def convertXmlDict(xml_file, xml_attribs=True):
    with open(xml_file, "rb") as f:  # notice the "rb" mode
        d = xmltodict.parse(f, xml_attribs=xml_attribs)
        return json.dumps(d, indent=4)


def loadConfig(conf):
    if os.path.isfile(conf):
        with open(conf) as data_file:
            configData = json.load(data_file)
    else:
        logging.error("main: Cannot find {config}.".format(config=conf))
        exit(1)

#############################################  Start Run  ##########################################
#  Set up logging
logging.basicConfig(filename='AMC_proc_fromS3_toDI.log', format='%(asctime)s::%(levelname)s::%(message)s',
                    level=logging_level)

logging.info("main: Started run")

################################################
#  Read and process XML files to jobs
jstr = ""
if useXml:
    if xmlUrl:  # Has a URL, presume remote
        logging.debug("main: using XML from:  {s}".format(s=xmlUrl))
        try:
            jstr = convertXmlDict(urlopen(xmlUrl))
        except URLError as e:
            logging.error("main: URL error.  Check internet connection")
            logging.error("main: {s}".format(s=e.reason))
            exit(1)
        except HTTPError as e:
            logging.error("main:  {s}".format(s=e.code))
            logging.error("main:  {s}".format(s=e.read))
            exit(1)
    else:
        logging.debug("main: using XML from:  {s}".format(s=xmlFile))
        jstr = convertXmlDict(xmlFile)

    logging.info("main: Cleaning invalid chars ('@' and '#')...")
    no_at_str = jstr.replace('"@', '"')
    jstr = no_at_str.replace('"#', '"')
    [s.strip() for s in jstr.splitlines() if s.strip()]

    jobsData = json.loads(jstr)

    logging.info("main: XML translated to:  {s}".format(s=jobsData))

else:
    if jobsUrl:  # If jobsURL is empty, use the local copy
        logging.debug("main: using JSON from:  {s}".format(s=jobsUrl))
        try:
            jobsData = json.load(urllib.urlopen(jobsUrl))
        except URLError as e:
            logging.error("main: URL error.  Check internet connection")
            logging.error("main:  {s}".format(s=e.reason))
            exit(1)
        except HTTPError as e:
            logging.error("main:  {s}".format(s=e.code))
            logging.error("main:  {s}".format(s=e.read))
            exit(1)
    else:  # open the copy local to the script
        logging.debug("main: using JSON from:  {s}".format(s=configfile))
        if os.path.isfile(configfile):
            with open(configfile) as data_file:
                jobsData = json.load(data_file)
        else:
            logging.error("main: Cannot find {jobs}.".format(jobs=configfile))
            exit(1)

try:
    conn = S3Connection(
        aws_access_key_id=aws_keyId,
        aws_secret_access_key=aws_sKeyId,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        # debug= 2
    )

    logging.debug("main: response for AWS connection attempt:  {r}".format(r=str(conn)))

except URLError as e:
    logging.fatal("main: URL error.  Check internet connection")
    logging.fatal("main: {s}".format(s=e.reason))
    exit(1)

except HTTPError as e:
    logging.fatal("main:  {s}".format(s=e.code))
    logging.fatal("main:  {s}".format(s=e.read))
    exit(1)

if jobsData:  # If we have a list of json-based jobs.
    #  This is a governor feature to limit the max number of concurrent BC jobs to 20.  Leave it.
    if len(jobsData) > 20:
        logging.warning("The maximum number of jobs allowed is 20.")
        exit(1)
    logging.debug("main: jobsData:  {s}".format(s=jobsData))

    # Build a list of files (keys) in the bucket
    mybucket = conn.get_bucket(aws_path, validate=True)

    mybucket.get_all_keys(maxkeys=0)  # maxkeys is for getting chunks in large trees; 0=all
    for key in mybucket.list():
        myFileList.append([key.name, key.size, key.last_modified])

    # Now we want to see if the list of files within jobs exist in myBucket
    logging.info("main: Found the following files at {p}:".format(p=aws_path))
    logging.info("main:  {s}".format(s=myFileList))

    cmsobj = {}
    # We are looking for "publisher-upload-manifest": - root of each job.
    # There will be one instance of "publisher-upload-manifest" for each job.
    for job in jobsData:
        #######################  Start Brightcove Ingestion - CMS data ######################################
        # We first have to test if the video exists (by name) as duplicates will be created otherwise.
        # NOTE: The logic here simply decides if a video by the same name already exists.
        videoobj = return_video_metadata_by_name(jobsData["publisher-upload-manifest"]["title"]["name"])
        logging.debug("main: contents of existing video query: {s}".format(s=videoobj))
        # Logic to handle the existance of a video by returning a query by name.
        if len(videoobj) == 0:  # If it is not there, make it.
            logging.debug("main: Call to find {s} returned null.  Building a new job.".format(
                s=jobsData["publisher-upload-manifest"]["title"]["name"]))
            # Create a new BC CMS object using metadata from jobs.json
            cmsobj['name'] = jobsData["publisher-upload-manifest"]["title"]["name"]
            cmsobj['description'] = jobsData["publisher-upload-manifest"]["title"]["short-description"]
            cmsobj['long_description'] = jobsData["publisher-upload-manifest"]["title"]["long-description"]
            cmsobj['tags'] = jobsData["publisher-upload-manifest"]["title"]["tag"]
            cmsobj['reference_id'] = jobsData["publisher-upload-manifest"]["title"]["refid"]
            cmsobj['state'] = "INACTIVE"

            yoursdate = dateutil.parser.parse(jobsData["publisher-upload-manifest"]["title"]["start-date"]).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            sdate = {'starts_at': yoursdate}
            cmsobj['schedule'] = sdate

            youredate = dateutil.parser.parse(jobsData["publisher-upload-manifest"]["title"]["end-date"]).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            edate = {'ends_at': youredate}
            cmsobj['schedule'] = edate


            #######################  Custom Fields  ############################################################
            cfobj = {}
            for cf in jobsData["publisher-upload-manifest"]["title"]["custom-string-value"]:
                try:
                    key = cf["name"]
                    value = cf["text"]
                    cfobj[key] = value  # todo:  need to add this object in
                except KeyError as e:
                    logging.warning("main: Key error in attempting to add: {s}".format(s=e.message))
                    continue
            cmsobj['custom_fields'] = cfobj


            #######################  Cue points  ############################################################
            cpc = []
            cpobj = {}
            for cp in jobsData["publisher-upload-manifest"]["cuepoint"]:
                cpobj = {'name': cp["name"],
                         'time': float(cp["time"]),
                         'type': cp["type"],
                         'metadata': cp["video-refid"]}
                cpc.append(cpobj)
            cmsobj['cue_points'] = cpc


            logging.debug("main.videoobj.cms object:  {s}".format(s=cmsobj))
            videoresponseobj = create_video(cmsobj)
            logging.debug("main.videoobj.create_video return:  {s}".format(s=videoresponseobj))

            if len(videoresponseobj) == 1:  # If it has only one error message in it.
                logging.debug("main.videoresponseobj:  {s}".format(s=videoresponseobj))
                continue

            else:  # From here we have the video ID and make the call for actual ingestion
                vid = videoresponseobj['id']
                logging.info("main.job.vid:  Using video ID {s}".format(s=vid))
                #######################  Start Brightcove Ingestion - dynamic ingestion ##########################
                r = submit_pbi(vid)  # This is where the DI work starts
                logging.debug("main.job.r:  {s}".format(s=r))
                logging.info("main.job.submit_pbi return:  {s}".format(s=r))
                '''
                if moveWhenComplete:
                bucket = conn.get_bucket(aws_path)
                for key in bucket:
                bucket.copy_key("complete/{nkey}".format(nkey=key), bucket, key)
                '''
                logging.info("main.job: Ending run")
                conn.close()
                exit(0)
        else:
            logging.info("main.job: A video by the name of {s} is already used.  Skipping this pass at ingest.".format(
                s=jobsData["publisher-upload-manifest"]["title"]["name"]))
            conn.close()
            exit(0)
else:
    logging.fatal("main: No data to process.")
    exit(1)

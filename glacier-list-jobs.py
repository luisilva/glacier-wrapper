#!/usr/bin/python
# Wes Dillingham / wes_dillingham@harvard.edu
# Provide this script with your amazon credentials and a job id and this script returns????

from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
from boto.glacier.job import Job
from boto.glacier.concurrent import ConcurrentUploader
import argparse,logging,os,csv,sys,json


class glacier_retrieve_jobs:
  def __init__(self):
      self.argparser = self.argparser()
      self.parsecreds = self.parsecreds()
      self.listjobs = self.listjobs()

  def argparser(self):
  #setting up parsing options for inputting data
      parser = argparse.ArgumentParser(description="Lists AWS Glacier initialized jobs")
      parser.add_argument("-c", "--creds", required=True, help="Path to Credential .csv file from amazon")
      parser.add_argument("-n", "--vault-name", required=True, help="Name of the AWS Glacier vault to inventory")
      parser.add_argument("-j", "--job-id", required=False, help="Name of the Job ID from the intiate job request, if none specified all jobs will be listed ")
      parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose output")
  
      args = parser.parse_args()

      self.creds = args.creds
      self.vault_name = args.vault_name
      self.job_id = args.job_id

      debug = args.verbose
      log_level = logging.INFO
      if debug == True:
          log_level = logging.DEBUG
      logging.basicConfig(filename="glacier_job_list.log", level=log_level, format=LOG_FORMAT)

      logger.debug(" ".join(sys.argv))

  def parsecreds(self): # getting credentials from CSV file assigning them to variables
      if not os.path.exists(self.creds):
          logger.critical("credential file does not exists => %s" % self.creds)
      else:
          with open(self.creds) as csvfile:
              reader = csv.DictReader(csvfile)
              for row in reader:
                  self.glacier_users = row['User Name']
                  self.glacier_key_id = row['Access Key Id']
                  self.glacier_secret_key = row['Secret Access Key']
                  
  def listjobs(self):
      glacier_layer1 = Layer1(aws_access_key_id=self.glacier_key_id, aws_secret_access_key=self.glacier_secret_key)
      logger.info("Listing Jobs associated with Job Id: => %s" % self.job_id)
      print("Grabbing list of jobs...");
      if(self.job_id != None):
          parsejson = glacier_layer1.get_job_output(self.vault_name, self.job_id)
          print json.dumps(parsejson, indent=4, sort_keys=True)
      else:
          parsejson = glacier_layer1.list_jobs(self.vault_name, completed=False)
          print json.dumps(parsejson, indent=4, sort_keys=True)
      


if __name__ == '__main__':
    # set log formatting
    LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
    logger = logging.getLogger('glacier_job_list.log')
    glacier_retrieve_jobs()
#!/usr/bin/python
# Wes Dillingham / wes_dillingham@harvard.edu
# Provive this script with your amazon download credential csv and a vault name-to get an output of
# your AWS Glacier archive ids.

from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
from boto.glacier.job import Job
from boto.glacier.concurrent import ConcurrentUploader
import argparse,logging,os,csv,sys,json


class glacier_retrieve_vault_list:
  def __init__(self):
      self.argparser = self.argparser()
      self.parsecreds = self.parsecreds()
      self.listvault = self.listvault()

  def argparser(self):
      #Setting up parsing options for inputting data
      parser = argparse.ArgumentParser(description="Initiates an AWS Glacier Vault Inventory")
      parser.add_argument("-c", "--creds", required=True, help="Path to Credential .csv file from amazon")
      parser.add_argument("-n", "--vault-name", required=True, help="Name of the AWS Glacier vault to inventory")
      parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose output")
      parser.add_argument("-p", "--proxy", required=False, default="rcproxy.rc.fas.harvard.edu:8888", help="override proxy settings default odyssey proxy setting are => rcproxy.rc.fas.harvard.edu:8888")
      #possibly add option for vault returns in formats other than JSON

      args = parser.parse_args()

      self.creds = args.creds
      self.verbose = args.verbose
      self.vault_name = args.vault_name
      self.proxy_address = args.proxy.split(':')[0]
      self.proxy_port = args.proxy.split(':')[1]

      debug = args.verbose
      log_level = logging.INFO
      if debug == True:
        log_level = logging.DEBUG
      logging.basicConfig(filename="glacier_vault_list.log", level=log_level, format=LOG_FORMAT)

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


  def listvault(self):
      glacier_layer1 = Layer1(aws_access_key_id=self.glacier_key_id, aws_secret_access_key=self.glacier_secret_key)
      logger.info("Inventory Job Id: => %s" % self.vault_name)
      print("Grabbing vault inventory operation starting...");
      logger.info("Grabbing vault inventory operation starting... => %s" % self.vault_name)
      job_id = glacier_layer1.initiate_job(self.vault_name, {"Description":"inventory-job", "Type":"inventory-retrieval", "Format":"JSON"})
      parsejson = json.dumps(job_id, indent=4, sort_keys=True)
      print("Inventory job id: %s"%(parsejson,));
      print("Amazon Glacier will take many hours to return the vault list, Provide the above Job ID to the Amazon Simple Notification System")
      print("located at: https://console.aws.amazon.com/sns to receive an email listing of your vault inventory. ")
      print("Job ID is valid for use 24 hours after Amazon completes job request")
      logger.info("Inventory Job Id: => %s" % self.vault_name)

if __name__ == '__main__':
    # set log formatting
    LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
    logger = logging.getLogger('glacier_vault_list.log')
    glacier_retrieve_vault_list()


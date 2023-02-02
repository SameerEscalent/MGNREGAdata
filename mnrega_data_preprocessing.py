# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 14:15:34 2023
"""
import pandas as pd
import warnings
import logging
from sys import exit
from datetime import datetime
import lxml

logging.basicConfig(level=logging.DEBUG, filename='log.txt', filemode='a',
        format ='%(asctime)s - %(levelname)s - %(message)s')

class MNREGADataLoader:
    def __init__(self):
        
        self.path = "."
        self.base_url = "https://mnregaweb4.nic.in/netnrega/Citizen_html/financialstatement.aspx"
        self.cols = ['S No.','State', 'Total Availablity', 'Unskilled Wage Expenditure', 'Material Expenditure', 'Admin Expenditure',
                     'Other Expenditure', 'Total Actual Exp', 'Actual Balance', 'Unskilled Wage Due', 'Material Due', 'Admin Due', 'Total Due', 
                     'Total Exp including payment due', 'Net Balance','Year']
        self.INDIA_ISO_CODES = {'ANDHRA PRADESH':  "IN-AP",'ARUNACHAL PRADESH':  "IN-AR",'ASSAM':  "IN-AS",'BIHAR':  "IN-BR",
                           'CHATTISGARH':  "IN-CT",'CHHATTISGARH':  "IN-CT",'GOA':  "IN-GA",'GUJARAT':  "IN-GJ",
                           'HARYANA':  "IN-HR",'HIMACHAL PRADESH':  "IN-HP",'JHARKHAND':  "IN-JH",'JHARKHAND#':  "IN-JH",
                           'KARNATAKA':  "IN-KA",'KERALA':  "IN-KL",'MADHYA PRADESH':  "IN-MP",'MADHYA PRADESH#':  "IN-MP",
                           'MAHARASHTRA':  "IN-MH",'MANIPUR':  "IN-MN",'MEGHALAYA':  "IN-ML",'MIZORAM':  "IN-MZ",
                           'NAGALAND':  "IN-NL",'NAGALAND#':  "IN-NL",'ODISHA':  "IN-OR",'PUNJAB':  "IN-PB",
                           'RAJASTHAN':  "IN-RJ",'SIKKIM':  "IN-SK",'TAMIL NADU':  "IN-TN",'TELENGANA':  "IN-TG",
                           'TELANGANA':  "IN-TG",'TRIPURA':  "IN-TR",'UTTARAKHAND':  "IN-UT",'UTTAR PRADESH':  "IN-UP",
                           'WEST BENGAL':  "IN-WB",'ANDAMAN AND NICOBAR ISLANDS':  "IN-AN",
                           'ANDAMAN & NICOBAR ISLANDS':  "IN-AN",'CHANDIGARH':  "IN-CH",'DADRA AND NAGAR HAVELI':  "IN-DN",
                           'DADRA & NAGAR HAVELI':  "IN-DN",'DADAR NAGAR HAVELI':  "IN-DN",'DAMAN AND DIU':  "IN-DD",
                           'DAMAN & DIU':  "IN-DD",'DELHI':  "IN-DL",'JAMMU AND KASHMIR':  "IN-JK",
                           'JAMMU & KASHMIR':  "IN-JK",'LADAKH':  "IN-LA",'LAKSHADWEEP':  "IN-LD",'LAKSHWADEEP':  "IN-LD",
                           'PONDICHERRY':  "IN-PY",'PUDUCHERRY':  "IN-PY",
                           'DADRA AND NAGAR HAVELI AND DAMAN AND DIU':  "IN-DH",'TELANGANA':  "IN-TG",'ALL INDIA':  "IN",
                           'DN HAVELI AND DD':  "IN-DN",'ANDAMAN AND NICOBAR':  "IN-AN"}
        self.data_list = []
        self.df = None
        self.dataset = pd.DataFrame()
    
    def download(self,start_year=2019,end_year=2023):
        'Download the required date from the web'
        warnings.filterwarnings('ignore')
        
        for yr in range(start_year,end_year):
            a=0
            try:
                while a<4:
                    try:
                        data = pd.read_html(f"{self.base_url}?lflag=eng&fin_year={yr}-{yr+1}&source=national&labels=labels&Digest=cN96LBEGlHkRAwn+MUntcQ")[4][3:].iloc[:-1]
                        data['Year']= f'{yr}' 
                        self.data_list.append(data)
                        self.df= pd.concat(self.data_list)
                        self.df.columns = self.cols
                        break
                    except:
                        a = a+1
            except:
                logging.debug('ENCOUNTERED ERROR IN DOWNLOADING DATA. Ended the Process at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                exit()
        logging.debug('Data Downloaded at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
    def process(self):
        'Process the data to bring it in the required format'
        try:
            for c in self.df.columns[2:]: 
                if c != 'Year':
                    self.df[c]= self.df[c].astype('float')
            # self.df['Cumulative_Expenditure_Year']= self.df.groupby('Year')['Total Actual Exp'].transform('sum').round(2)
            # self.df['Cumulative_Expenditure_State'] = self.df.groupby('State')['Total Actual Exp'].transform('sum').round(2)
            self.df["State"] = self.df["State"].apply(lambda x: self.INDIA_ISO_CODES[x])
            self.df.drop(['S No.', 'Actual Balance', 'Unskilled Wage Due', 'Material Due', 'Admin Due', 'Total Due', 
                         'Total Exp including payment due', 'Net Balance'], axis=1,inplace=True)
            self.df.columns = [col.lower().replace(" ", "_") for col in self.df.columns]
        except:
            logging.debug('ERROR IN PROCESSING at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            exit()
        logging.debug('Data Processed at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    
    def save(self, filename=None, index=False):
        'Saving the data in the CSV format'
        try:
            self.df.to_csv('{}/{}'.format(self.path, filename), index=index)
        except:
            logging.debug("ERROR IN SAVING at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            exit()
        logging.debug("Data saved in CSV at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
def main():
    'Runs the main function'
    logging.debug('Started the Process at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    loader = MNREGADataLoader()
    loader.download()
    loader.process()
    loader.save(filename='mnrega_data.csv')
    logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    main()

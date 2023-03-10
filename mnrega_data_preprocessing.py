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

logging.basicConfig(level=logging.DEBUG, filename='./log.txt', filemode='a',
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
        'Download the required data from the web'
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
            self.df.drop(['S No.', 'Actual Balance', 'Unskilled Wage Due', 'Material Due', 'Admin Due', 'Total Due', 
                         'Total Exp including payment due', 'Net Balance'], axis=1,inplace=True)
            self.df.columns = [col.lower().replace(" ", "_") for col in self.df.columns]
            for c in self.df.columns: 
                if (c != 'year') & (c != 'state'):
                    self.df[c]= self.df[c].astype('float')
                    self.df[c]= self.df[c]*100000
            self.df["state"] = self.df["state"].apply(lambda x: self.INDIA_ISO_CODES[x])
            logging.debug('Data Processed at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            return self.df
        except:
            logging.debug('ERROR IN PROCESSING at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            exit()
    
    def save(self, data=None, filename=None, index=False):
        'Saving the data in the CSV format'
        try:
            if data is None:
                data = self.df
            else:
                data=data
            data.to_csv('{}/{}'.format(self.path, filename), index=index)
            logging.debug("Data saved successfully at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except:
            logging.debug("ERROR IN SAVING at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            exit()
        logging.debug("Data saved in CSV at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
    def budget_by_year_state(self):
        'State and Year wise Expenditure'
        yearly_spend = self.df.groupby('year')['total_actual_exp'].sum().to_frame().rename(columns={'total_actual_exp': 'budget_spent'})
        state_wise_spend = self.df.groupby('state')['total_actual_exp'].sum().to_frame().rename(columns={'total_actual_exp': 'budget_spent'})
        self.save(data=yearly_spend,filename='yearly_spend.csv',index=True)
        self.save(data=state_wise_spend,filename='state_wise_spend.csv',index=True)
        yearly_state_wise_spend = self.df.groupby(['state', 'year'])['total_actual_exp'].sum().to_frame().rename(columns={'total_actual_exp': 'budget_spent'})
        self.save(data=yearly_state_wise_spend,filename='yearly_state_wise_spend.csv',index=True)
        
def main():
    'Runs the main function'
    warnings.filterwarnings('ignore')
    logging.debug('Started the Process at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    loader = MNREGADataLoader()
    loader.download()
    loader.process()
    loader.save(filename='mnrega_data.csv')
    loader.budget_by_year_state()
    logging.debug("Ended the Process at {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    main()

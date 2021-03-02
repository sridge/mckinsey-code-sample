import gzip
import shutil
import os
import pandas as pd
import requests
from joblib import Parallel, delayed

class isd_lite:

    def __init__(self,year,file_outpath,
                 base_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/',
                 fname_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv',
                ):

        self.base_url = base_url
        self.fname_url = fname_url
        self.year = year
        self.fname_list = []
        self.file_outpath = file_outpath 
        
    # def get_date_modified(self):
    #     return date_modified

    def read_filenames(self,us_only=True):

        df = pd.read_csv(self.fname_url)

        if us_only:
            df = df[df['STATE'].notnull()]

        df['fname'] = df['USAF'].astype(str) + '-' + df['WBAN'].astype(str) + f'-{self.year}.gz'
        
        self.fname_list = list(df['fname'])

        return list(df['fname'])
    
    def decompress(self,fname):
        
        with gzip.open(f'{self.file_outpath}{fname}', 'rb') as f_in:
            with open(f'{self.file_outpath}{fname[0:-3]}.csv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            
        os.remove(f'{self.file_outpath}{fname}')

    def download(self,fname,decompress_file=True):

        url = f'{self.base_url}/{self.year}/{fname}'

        r = requests.get(url)
        
        if r.status_code != 404:
            
            with open(f'{self.file_outpath}{fname}', 'wb') as f:
                f.write(r.content)
                
            if decompress_file:

                self.decompress(fname)
                
    
    def download_all(self,n_jobs=10,):

        self.n_jobs = n_jobs
        Parallel(n_jobs=self.n_jobs)(delayed(self.download)(fname) for fname in self.fname_list)
    


        

        




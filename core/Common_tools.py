# -*- coding: utf-8 -*-


"""

"""

import pandas as pd 
import numpy as np 


__version__ = '0.0.1'




class Basesteps(object):

      def __init__(self,data,
                        target,
                        ignore_columns=None):
          self.data=data.copy()
          self.target=target
          self.ignore_columns=ignore_columns[:]
          if not isinstance(self.data,pd.DataFrame):
             raise ValueError('''The data isn't pandas DataFrame type,please check it''')

          if ignore_columns:
             self.ignore_func(self.ignore_columns)



      def ignore_func(self,ignore_columns):
          self.recover=self.data[ignore_columns]
          self.data.drop(ignore_columns,1,inplace=True)
          ignore_columns.remove(self.target.name)
          self.ignore_columns=ignore_columns 



      def recover_func(self,final_df):
          final_df[self.target.name]=self.target
          for i,l in enumerate(self.ignore_columns):
                 final_df.insert(i,l,self.recover[l])
          return  final_df

          
      def get_columns(self,column):
          if column:
             if column=='ALL':
                name=self.data.columns
             else:
                if not isinstance(column,list):
                   raise ValueError('column must be a list or tuple') 
                name=column[:]
          else:
              name=None
          return name 




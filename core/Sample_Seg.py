# -*- coding: utf-8 -*-


"""

"""

import math
import pandas as pd 
import numpy as np 
from sklearn.model_selection import train_test_split
from Common_tools import Basesteps


__version__ = '0.0.1'




class Sample(Basesteps):
       
      def __init__(self,
                    data,
                    target,
                    pct_train=0.8,
                    class_weight='balanced',
                    drop_columns=[]):
          super(Sample,self).__init__(data,target)
          self.pct_train=pct_train
          self.class_weight=class_weight
          self.drop_columns=drop_columns


      def resample(self):
          df=self.data.copy()
          df_good=df.query('{}==0'.format(self.target.name))
          df_bad=df.query('{}==1'.format(self.target.name))
          ori_weight={0:math.ceil(len(df_good)*1.0/len(df_bad)),1:1.0}
        
          def _down_sample(specific_weight):
              pct=round(specific_weight*1.0/ori_weight[0],3)
              return df_good.sample(frac=pct)
            
          def _up_sample(df_bad):                                ##为什么用global会失效？
              bad_copy=df_bad.copy()
              for i in range(1,self.class_weight[1]):
                  df_bad=pd.concat([df_bad,bad_copy])
              return df_bad
          
          if  isinstance(self.class_weight, dict):
              specific_weight=self.class_weight[0]
              return pd.concat([_up_sample(df_bad),_down_sample(specific_weight)])

          else:
              specific_weight=1
              return pd.concat([df_bad,_down_sample(specific_weight)])

 
      def train_test_split(self,df):
          train,test=train_test_split(df,train_size=self.pct_train)
          return train,test 


      def drop(self,df):
          return df.drop(self.drop_columns,1)


      def __call__(self):
          return self.train_test_split(self.drop(self.resample()))


class Segmentation(Basesteps):

      def __init__(self,data,condition):
           super(Segmentation,self).__init__(data,None)
           self.condition=condition


    
      def  segment(self):
           df=self.data.copy()
           df.columns = df.columns.map(lambda x: self.operator_repalce(x))
           eval_columns=''''''
           _group=[]
           for i,l in enumerate(self.condition):
               if [y for y in [l.count(x) for x in ['<','>','='] ] if y >=2]: 
                  l=self.operator_repalce(l)
               l=l.replace('equal=','==')
               eval_columns=eval_columns+'condition'+str(i)+'='+l+'\n'
               _group.append('condition'+str(i))
           df.eval(eval_columns)
           grouped=df.groupby(_group)
           return grouped,_group

      def  operator_repalce(self,_str):
           _str=_str.replace('>','bigger',1)
           _str=_str.replace('<','smaller',1)
           _str=_str.replace('=','equal',1)
           return _str

      def  operator_recover(self,_str):
           _str=_str.replace('bigger','>',1)
           _str=_str.replace('smaller','<',1)
           _str=_str.replace('equal','=',1)
           return _str


      def  get_groups(self,list):
           grouped,_group=self.segment()
           final_data=grouped.get_group(list).drop(_group,1)
           final_data.columns = final_data.columns.map(lambda x: self.operator_recover(x))
           return final_data
      
      @property
      def  get_all(self):
           grouped,_group=self.segment()
           final_dict={}
           for i in grouped:
               final_data=i[1].drop(_group,1)
               final_data.columns = final_data.columns.map(lambda x: self.operator_recover(x))
               final_dict[i[0]]=final_data
           return final_dict   













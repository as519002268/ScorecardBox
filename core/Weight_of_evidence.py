# -*- coding: utf-8 -*-


"""
Use pandas calculate woe value and information value
"""

import pandas as pd 
import numpy as np 


__version__ = '0.0.1'


class Woe(object):

      def __init__(self,series,target):
          self.series=pd.Series(series)
          self.target=pd.Series(target)
          self._max_woe=3
          self._min_woe=-3

          self.woe_iv(self.series,self.target)


      def woe_iv(self,series,flag):
          table=self.woe_table(series,flag)
          bad_total=table[1].sum()
          good_total=table[0].sum()
          table['woe']=table.apply(lambda x:self.woe_calculate(x,good_total,bad_total),axis=1)
          table['iv']=(table[1]/bad_total-table[0]/good_total)*table.woe
          self.table=table


      def woe_calculate(self,table,gt,bt):
          good,bad=table.values
          if good==0:
             return self._max_woe
          elif bad==0:
             return self._min_woe
          else:
             return round(np.log((bad*1.0/bt)/(good*1.0/gt)),4)

      def woe_table(self,series,flag):
          one=pd.Series(1,index=range(0,len(series)))
          _g=pd.concat([series,flag,one],axis=1)
          table=_g.groupby([series.name,flag.name]).sum().unstack()
          table.columns=[0,1]
          return table

      @property
      def woe(self):
          return self.table.woe       

      @property
      def iv(self):
          return round(sum(self.table.iv),4)  

      @property
      def max_woe(self):
          return self._max_woe
      @max_woe.setter
      def max_woe(self, max_woe):
          self.woe_iv(self.series,self.target)
          self._max_woe = max_woe

      @property
      def min_woe(self):
          return self._min_woe
      @min_woe.setter
      def min_woe(self, min_woe):
          self.woe_iv(self.series,self.target)
          self._min_woe = min_woe


class Woe_dataframe(object):
       
      def __init__(self,data,
                       target,
                       columns='ALL',
                       ignore_columns=None):

          if not isinstance(data,pd.DataFrame):
             raise ValueError('''The data isn't pandas DataFrame type,please check it''')

          self.data=data.copy()
          self.target=target
          self.columns=self.get_columns(columns)

          if ignore_columns:
             self.ignore_func(ignore_columns)

      def ignore_func(self,ignore_columns):
          self.recover=self.data[ignore_columns]
          self.data.drop(ignore_columns,1,inplace=True)
          ignore_columns.remove(self.target.name)
          self.ignore_columns=ignore_columns


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


      @property
      def get_woe(self):
          woe_dict={}
          for i in self.columns:
              woe_dict[i]=Woe(self.data[i],self.target).woe
          return  woe_dict  

      @property
      def get_iv(self):
          iv_dict={}
          for i in self.columns:
              iv_dict[i]=Woe(self.data[i],self.target).iv
          return  iv_dict

      @property
      def get_table(self):
          table_dict={}
          for i in self.columns:
              table_dict[i]=Woe(self.data[i],self.target).table
          return  table_dict 

      def woe_transform(self,woe_dict):
          transform=self.data[woe_dict.keys()].apply(
                                lambda x:self.trans_func(woe_dict,x),
                                axis=0)
          transform[self.target.name]=self.target
          for i,l in enumerate(self.ignore_columns):
              transform.insert(i,l,self.recover[l])
          return transform
          

      def trans_func(self,woe_dict,series):
          transform_temp=woe_dict[series.name] 
          ori_data=self.data[series.name]
          transformed=transform_temp.loc[ori_data]
          transformed.index=ori_data.index
          return transformed
       
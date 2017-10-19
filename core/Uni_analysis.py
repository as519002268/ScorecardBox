# -*- coding: utf-8 -*-


"""
Use pandas calculate woe value and information value
"""

import pandas as pd 
import numpy as np 
import Weight_of_evidence as we


__version__ = '0.0.1'


class Univariable(object):

      def __init__(self,data,target,columns='ALL',ignore_columns=None):
          self.data=data.copy()
          self.target=target
          self.columns=columns
          self._iv_threshold=0.02

          if ignore_columns:
             self.ignore_func(ignore_columns)

          self.ins=we.Woe_dataframe(self.data,self.target,self.columns)
          self.analysis_table()

      def ignore_func(self,ignore_columns):
          self.recover=self.data[ignore_columns]
          self.data.drop(ignore_columns,1,inplace=True)
          ignore_columns.remove(self.target.name)
          self.ignore_columns=ignore_columns


      def analysis_table(self):
          iv=self.ins.get_iv
          uni_table=pd.DataFrame(iv.values(),index=iv.keys(),columns=['Iv'])
          uni_table=uni_table.sort_values('Iv',ascending=False)
          uni_table['Iv_rank']=uni_table.rank(ascending=False)
          self.uni_table=uni_table


      @property
      def drop_columns(self):
          drop_col=self.uni_table.query('Iv<%s'%(self._iv_threshold))
          return drop_col


      @property
      def iv_threshold(self):
          return self._iv_threshold

      @iv_threshold.setter
      def iv_threshold(self, iv_threshold):
          self._iv_threshold = iv_threshold    


      def drop(self):
          drop_col=self.drop_columns
          droped=self.data.drop(drop_col.index.values,1)
          droped[self.target.name]=self.target
          for i,l in enumerate(self.ignore_columns):
              droped.insert(i,l,self.recover[l])
          return droped




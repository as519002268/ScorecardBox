# -*- coding: utf-8 -*-


"""
Use pandas calculate woe value and information value
"""

import pandas as pd 
import numpy as np 
import Weight_of_evidence as we
from Common_tools import Basesteps

__version__ = '0.0.2'


class Univariable(Basesteps):

      def __init__(self,data,
                        target,
                        ignore_columns=None):
          super(Univariable,self).__init__(data,target,ignore_columns)
          self._iv_threshold=0.02
          self.ins=we.Woe_dataframe(self.data,self.target,'ALL')
          self.analysis_table()


      def analysis_table(self):
          iv=self.ins.get_iv
          uni_table=pd.DataFrame(iv.values(),index=iv.keys(),columns=['Iv'])
          uni_table=uni_table.sort_values('Iv',ascending=False)
          uni_table['Iv_rank']=uni_table.rank(ascending=False)
          self.uni_table=uni_table


      @property
      def drop_columns(self):
          drop_col=self.uni_table.query('Iv<%s'%(self._iv_threshold))
          return pd.concat([drop_col,self.uni_table[self.uni_table['Iv'].isnull()]])


      @property
      def iv_threshold(self):
          return self._iv_threshold

      @iv_threshold.setter
      def iv_threshold(self, iv_threshold):
          self._iv_threshold = iv_threshold    


      def drop(self):
          drop_col=self.drop_columns
          droped=self.data.drop(drop_col.index.values,1)
          if self.ignore_columns:
             droped=self.recover_func(droped)
          return droped




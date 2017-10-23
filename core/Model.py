# -*- coding: utf-8 -*-


"""
"""

import pandas as pd 
import numpy as np 
from sklearn.feature_selection import RFE
from sklearn.feature_selection import RFECV
from sklearn.cross_validation import StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import RandomizedLogisticRegression
from sklearn.grid_search import GridSearchCV



__version__ = '0.0.1'


class Models(object):

      def __init__(self,data,target,
                   ignore_columns=None,
                   C=0.5,
                   class_weight='balanced'):
          self.data=data.copy()
          self.target=target
          self.C=C
          self.class_weight=class_weight
          self.lr = LogisticRegression(penalty='l1',
                                       class_weight=class_weight,
                                       C=C,
                                       n_jobs=-1,random_state=1)

          if ignore_columns:
             self.data.drop(ignore_columns,1,inplace=True)

      def rfe(self,n_features_to_select):
          rfe_model = RFE(self.lr,
                          n_features_to_select=n_features_to_select,
                          step=1)
          rfe_model.fit(self.data,self.target)
          return   rfe_model


      def rfecv(self,cv_scoring='roc_auc',n_KFold=3):
          rfecv_model=RFECV(estimator=self.lr, 
                            step=1, 
                            cv=StratifiedKFold(self.target,n_KFold), 
                            scoring=cv_scoring,n_jobs=-1)
          rfecv_model.fit(self.data,self.target)
          return   rfecv_model


      def randlogistic(self,selection_threshold=0.25,sample_fraction=0.75):
          rlr_model = RandomizedLogisticRegression(C=self.C,
                                                   selection_threshold=selection_threshold,
                                                   normalize=False,
                                                   sample_fraction=sample_fraction)
          rlr_model.fit(self.data.values,self.target.values)
          return rlr_model


      def LR(self,cv_scoring='roc_auc',cv=3):
          estimator=LogisticRegression(penalty='l2',
                                       class_weight=self.class_weight,
                                       n_jobs=-1)
          param_grid={'C':np.linspace(0.1,1,100)}
          grid_search = GridSearchCV(estimator=estimator,
                                     param_grid=param_grid,
                                     scoring=cv_scoring,
                                     cv=cv,
                                     n_jobs=-1)
          grid_search.fit(self.data,self.target)
          best_parameters = grid_search.best_estimator_.get_params()
          print 'the best estimator is:\n'
          print  grid_search.best_estimator_
          print  best_parameters
          lr_model = LogisticRegression(penalty='l2',
                                        class_weight=self.class_weight,
                                        n_jobs=-1,
                                        C=best_parameters['C'])
          lr_model.fit(self.data,self.target)  
          return lr_model



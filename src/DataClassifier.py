"""
`DataClassifier` class contains methods for modeling,
classifying, and evaluating NBA game prediction methods.

Attributes:
    load_data(): Loads complete preprocessed data set.
    
    set_feats_and_labels(feats, labels, skip_playoffs,
                         start_year, end_year): 
        Selects features and labels for modeling. 
    
    set_train_test_split(n_splits, test_size, rng_seed): Sets data
        shuffling parameters.
    
    set_classifiers(classifiers): Sets classifiers from scikit-learn.
    
    train_and_test_models(verbose): Trains and tests/evaluates
        all classification models.  Shuffles data for 
        cross-validation using StratifiedShuffleSplit 
        (stratified k-fold with shuffling). Logs results.
        
    plot_results(): Plots classifier accuracy and log loss.

    Requirements:
        sklearn, pandas, numpy, seaborn, matplotlib
"""

from sklearn.metrics import accuracy_score, log_loss
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC, LinearSVC, NuSVC
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, \
                             GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.linear_model import LogisticRegression
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import datetime


# Hides sklearn warnings for nice printing
def warn(*args, **kwargs): pass
import warnings
warnings.warn = warn
from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


class DataClassifier:        
    def __init__(self, proc_dir = "data_preprocessed"):
        self.proc_team_file_path = proc_dir + "/team_box_scores/"
        datetime_now = datetime.datetime.now()
        self.date_today = datetime.date(datetime_now.year,\
                                       datetime_now.month,\
                                       datetime_now.day)
        self.team_full_df = pd.DataFrame()
        self.set_feats_and_labels()
        self.set_train_test_split()
        self.set_classifiers()
                
            
    def load_data(self):
        print("\nLoading model from processed data.\n")
        processed_complete_file_path = "./" + self.proc_team_file_path \
                                       + "complete_processed_team_box.csv"
        self.team_full_df = pd.read_csv(processed_complete_file_path)
    
        
    def set_feats_and_labels(self, feats = ["attempted_field_goals", \
                            "field_goal_percentage",\
                            "three_point_percentage",\
                            "made_free_throws",\
                            "defensive_rebounds",\
                            "total_rebounds",
                            "turnovers",\
                            "personal_fouls"], \
                            labels = ["outcome"], \
                            skip_playoffs = True, \
                            start_year = 2007, \
                            end_year = []):
        self.feats = feats
        self.labels = labels
        self.skip_playoffs = skip_playoffs
        self.start_year = start_year
        if not end_year or end_year > self.date_today.year:
            if self.date_today.month < 10:
                self.end_year = end_year - 1
            else:
                self.end_year = end_year
        else:
            self.end_year = end_year
        
    
    def set_train_test_split(self, n_splits=10, test_size = 0.2, rng_seed=0):
        self.n_splits = n_splits
        self.test_size = test_size
        self.rng_seed_init = rng_seed
        
        
    def set_classifiers(self, classifiers = ["KNN", 
                                             "SVC", \
                                             "NSVC", \
                                             "DTC", \
                                             "RFC", \
                                             "ABC",\
                                             "GBC", \
                                             "GNB", \
                                             "LDA", \
                                             "QDA", \
                                             "LR"]):
        self.classifiers = classifiers
    
        
    def train_and_test_models(self, verbose=True):
        
        self.X = self.team_full_df.loc[:, self.feats]
        self.y = self.team_full_df.loc[:, self.labels]
        
        self.accuracy_arr = np.zeros((self.n_splits,))
        self.ll_arr = np.zeros((self.n_splits,))
        
        
        self.log_cols=["Classifier", "Accuracy", "Log Loss"]
        self.log = pd.DataFrame(columns=self.log_cols)
        
        for clf_str in self.classifiers:
            for idx_model in range(0, self.n_splits):
                X_train, X_test, y_train, y_test \
                    = self.__get_train_test_split(self.rng_seed_init + idx_model)
                
                clf, name = self.__get_classifier(clf_str)
                #print("Training and testing classifier " \
                #      + name + " for model :",\
                #      idx_model+1, "of", self.n_splits)
                clf = clf.fit(X_train, y_train)
                
                train_predictions = clf.predict(X_test)
                self.accuracy_arr[idx_model] = accuracy_score(y_test, train_predictions)
                
                train_predictions = clf.predict_proba(X_test)
                self.ll_arr[idx_model] = log_loss(y_test, train_predictions)
                    
            if verbose:
                acc_temp = np.mean(self.accuracy_arr)
                var_temp = np.var(self.accuracy_arr)
                ll_temp = np.mean(self.ll_arr)
                
                print("="*30)
                print(name)
                print('****Results****')
                print("Accuracy : {:.4%}".format(acc_temp))
                if self.n_splits > 1:
                    print("Variance : {:.6}".format(var_temp))
                print("Log Loss : {:.8}".format(ll_temp))

            log_entry = pd.DataFrame([[name, acc_temp*100, ll_temp]], columns=self.log_cols)
            self.log = self.log.append(log_entry)

        if verbose:
            print("="*30)
            
            
    def plot_results(self):
        sns.set_color_codes("muted")
        sns.barplot(x='Accuracy', y='Classifier', data=self.log, color="b")

        plt.xlabel('Accuracy %')
        plt.title('Classifier Accuracy')
        plt.show()

        sns.set_color_codes("muted")
        sns.barplot(x='Log Loss', y='Classifier', data=self.log, color="g")

        plt.xlabel('Log Loss')
        plt.title('Classifier Log Loss')
        plt.show()
    
    
    def __get_classifier(self, clf_str):
        if clf_str is "KNN":
            clf = KNeighborsClassifier(3)
            name = clf.__class__.__name__
        elif clf_str is "SVC":
            clf = SVC(kernel="rbf", C=0.025, probability=True)
            name = clf.__class__.__name__
        elif clf_str is "NSVC":
            clf = NuSVC(probability=True)
            name = clf.__class__.__name__
        elif clf_str is "DTC":
            clf = DecisionTreeClassifier()
            name = clf.__class__.__name__
        elif clf_str is "DTR":
            clf = DecisionTreeRegressor()
            name = clf.__class__.__name__
        elif clf_str is "RFC":
            clf = RandomForestClassifier()
            name = clf.__class__.__name__
        elif clf_str is "ABC":
            clf = AdaBoostClassifier()
            name = clf.__class__.__name__
        elif clf_str is "GBC":
            clf = GradientBoostingClassifier()
            name = clf.__class__.__name__
        elif clf_str is "GNB":
            clf = GaussianNB()
            name = clf.__class__.__name__
        elif clf_str is "LDA":
            clf = LinearDiscriminantAnalysis()
            name = clf.__class__.__name__
        elif clf_str is "QDA":
            clf = QuadraticDiscriminantAnalysis()
            name = clf.__class__.__name__
        elif clf_str is "LR":
            clf = LogisticRegression()
            name = clf.__class__.__name__
        
        return clf, name
        
        
    def __get_train_test_split(self, rng_seed=0):
        sss = StratifiedShuffleSplit(n_splits=1, test_size=self.test_size, \
                                     random_state=rng_seed)
        for train_index, test_index in sss.split(self.X, self.y):
            X_train, X_test = self.X.iloc[train_index, :], self.X.iloc[test_index, :]
            y_train, y_test = self.y.iloc[train_index, :], self.y.iloc[test_index, :]
        return X_train, X_test, y_train, y_test
        
        
        
        
        
         
                      
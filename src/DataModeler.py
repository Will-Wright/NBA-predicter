# Loads preprocessed NBA game data and 
# builds predictions with various algorithms / machine learning models
#

from sklearn import tree
import numpy as np
import pandas as pd
import os
import datetime

class DataModeler:        
    def __init__(self, proc_dir = "data_preprocessed"):
        self.proc_team_file_path = proc_dir + "/team_box_scores/"
        datetime_now = datetime.datetime.now()
        self.date_today = datetime.date(datetime_now.year,\
                                       datetime_now.month,\
                                       datetime_now.day)
        self.team_full_df = pd.DataFrame()
        self.set_feats_and_label()
        self.set_training_params()
                
            
    def load_data(self):
        print("\nLoading model from processed data.\n")
        processed_complete_file_path = "./" + self.proc_team_file_path \
                                       + "complete_processed_team_box.csv"
        self.team_full_df = pd.read_csv(processed_complete_file_path)
    
        
    def set_feats_and_label(self, feats = ["attempted_field_goals", \
                            "field_goal_percentage",\
                            "three_point_percentage",\
                            "made_free_throws",\
                            "defensive_rebounds",\
                            "total_rebounds",
                            "turnovers",\
                            "personal_fouls"], \
                            label = ["outcome"], \
                            skip_playoffs = True, \
                            start_year = 2007, \
                            end_year = []):
        self.feats = feats
        self.label = label
        self.skip_playoffs = skip_playoffs
        self.start_year = start_year
        if not end_year:
            if self.date_today.month < 10:
                self.end_year = end_year - 1
            else:
                self.end_year = end_year
        else:
            self.end_year = end_year
        
        
    def set_training_params(self, train_ratio = 0.8, n_models=10):
        self.train_ratio = train_ratio
        self.n_models = n_models
        
        
    def train_and_test_model(self, model_type="decision_tree_classifier", rng_seed=0):
        self.model_type = model_type
        self.accuracy_arr = np.zeros((self.n_models,))
        
        
        for idx_model in range(0, self.n_models):
            print("Training and testing model :", idx_model+1, "of", self.n_models)
            
            np.random.seed(seed=rng_seed+idx_model)

            X = self.team_full_df.loc[:, self.feats]
            y = self.team_full_df.loc[:, self.label]

            n_samples = X.shape[0]
            rand_idx = np.arange(n_samples)
            np.random.shuffle(rand_idx)
            n_train = int(np.round(self.train_ratio*n_samples))
            n_test = n_samples - n_train
            train_idx = rand_idx[:n_train]
            test_idx = rand_idx[n_train:]

            X_train = X.loc[train_idx, :]
            X_test = X.loc[test_idx, :]
            y_train = y.loc[train_idx, :]
            y_test = y.loc[test_idx, :]

            if self.model_type == "decision_tree_classifier":
                clf = tree.DecisionTreeClassifier()
                clf = clf.fit(X_train, y_train)

                # Test accuracy
                n_correct = 0
                for idx_test in range(0,n_test):
                    if clf.predict([X_test.iloc[idx_test,:]])[0] \
                                == y_test.iloc[idx_test][0]:
                        n_correct = n_correct + 1
            elif self.model_type == "decision_tree_regressor":
                outcome_type = {'win': 1, 'loss': 0}
                y_train_binary = [outcome_type[item] for item in y_train.outcome]
                y_test_binary = [outcome_type[item] for item in y_test.outcome]

                clf = tree.DecisionTreeRegressor()
                clf = clf.fit(X_train, y_train_binary)
                # Test accuracy
                n_correct = 0
                for idx_test in range(0,n_test):
                    if abs( clf.predict([X_test.iloc[idx_test,:]])[0] \
                                - y_test_binary[idx_test] ) \
                            < 0.5 :
                        n_correct = n_correct + 1
            else:
                print("No valid model chosen")
                clf = []
                n_correct = 0
                
            self.accuracy_arr[idx_model] = n_correct / n_test

        print("Mean : %1.6f" %np.mean(self.accuracy_arr))
        print("Var  : %1.6f" %np.var(self.accuracy_arr))
        
        
        
        
        
        
        
        
        
        
        
        
        
        
         
                      
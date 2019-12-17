# Loads preprocessed NBA game data and 
# builds predictions with various algorithms / machine learning models
#

from sklearn import tree
import numpy as np
import pandas as pd
import os
import datetime

class DataModeler:        
    def __init__(self, feature_cols = ["attempted_field_goals", \
                                        "field_goal_percentage",\
                                        "three_point_percentage",\
                                        "made_free_throws",\
                                        "defensive_rebounds",\
                                        "total_rebounds",
                                        "turnovers",\
                                        "personal_fouls"], \
                 label_col = ["outcome"], \
                 start_year = 2007, end_year = 2019, \
                 proc_dir = "data_preprocessed"):
        self.feature_cols = feature_cols
        self.label_col = label_col
        self.start_year = start_year
        self.end_year = end_year
        self.proc_team_file_path = proc_dir + "/team_box_scores/"
        datetime_now = datetime.datetime.now()
        self.date_today = datetime.date(datetime_now.year,\
                                       datetime_now.month,\
                                       datetime_now.day)
        self.team_full_df = pd.DataFrame()

                
    def build_model(self):
        print("\nBuilding model from processed data.\n")
        year_range = range(self.start_year, self.end_year+1, 1)

        for year in year_range:
            print("Loading game data for season " + str(year)\
                  + "-" + str(year+1))
            season_str = str(year) + "_" + str(year + 1)
            date_season_current = datetime.date(year, 10, 1)
            date_season_end =  datetime.date(year+1, 3, 31)
            
            while date_season_current <= date_season_end \
                and date_season_current <= self.date_today:

                # Creates output path
                if date_season_current.day < 10:
                    day_str = "0" + str(date_season_current.day)
                else:
                    day_str = str(date_season_current.day)
                if date_season_current.month < 10:
                    month_str = "0" + str(date_season_current.month)
                else:
                    month_str = str(date_season_current.month)

                processed_temp_file_path = "./" + self.proc_team_file_path \
                    + season_str + "/" + str(date_season_current.year) \
                    + "_" + month_str + "_" + day_str + "_" + "team_box_scores.csv"

                if os.path.exists(processed_temp_file_path):
                    team_df = pd.read_csv(processed_temp_file_path)
                    self.team_full_df \
                      = pd.concat([self.team_full_df, team_df]).reset_index(drop=True)
                    
                date_season_current = date_season_current \
                                      + datetime.timedelta(days = 1)    
                    
        # end for year in year_range

        
        # Adds derived features to pd.DataFrame
        if "field_goal_percentage" in self.feature_cols:
            self.team_full_df["field_goal_percentage"] \
                = self.team_full_df["made_field_goals"] \
                  / (self.team_full_df["made_field_goals"] \
                     + self.team_full_df["attempted_field_goals"])
        if "three_point_percentage" in self.feature_cols:
            self.team_full_df["three_point_percentage"] \
                = self.team_full_df["made_three_point_field_goals"] \
                  / (self.team_full_df["made_three_point_field_goals"] \
                     + self.team_full_df["attempted_three_point_field_goals"])
        if "total_rebounds" in self.feature_cols:
            self.team_full_df["total_rebounds"] \
                = self.team_full_df["offensive_rebounds"] \
                  + self.team_full_df["defensive_rebounds"]
        

        
    
         
                      
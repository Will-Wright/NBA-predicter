# Collects and preprocesses NBA game data
#
# Requires the following web scraper utility
#    https://github.com/jaebradley/basketball_reference_web_scraper

import os
import csv
import datetime
import pandas as pd
import numpy as np
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import OutputType   

class DataProcessor:        
    def __init__(self, root_dir = "data_raw", proc_dir = "data_preprocessed"):
        self.player_file_path = root_dir + "/player_box_scores/"
        self.team_file_path = root_dir + "/team_box_scores/"
        self.season_file_path = root_dir + "/season_schedule/"
        self.proc_team_file_path = proc_dir + "/team_box_scores/"
        datetime_now = datetime.datetime.now()
        self.date_today = datetime.date(datetime_now.year,\
                                       datetime_now.month,\
                                       datetime_now.day)
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)
        if not os.path.exists(self.player_file_path):
            os.mkdir(self.player_file_path)
        if not os.path.exists(self.team_file_path):
            os.mkdir(self.team_file_path)
        if not os.path.exists(self.season_file_path):
            os.mkdir(self.season_file_path)
        if not os.path.exists(proc_dir):
            os.mkdir(proc_dir)
        if not os.path.exists(self.proc_team_file_path):
            os.mkdir(self.proc_team_file_path)

    def scrape_data_player_box_scores(self):
        print("\nScraping player box score data.\n")
        if self.date_today.month < 10:
            current_season_start_year = self.date_today.year-1
        else:
            current_season_start_year = self.date_today.year
        year_range = range(1999, current_season_start_year+1, 1)
        for year in year_range:
            season_str = str(year) + "_" + str(year + 1)
            if not os.path.exists(self.player_file_path + season_str):
                os.mkdir(self.player_file_path + season_str)

            status_path = self.player_file_path + season_str \
                          + '/season_data_status.csv'
            is_complete_season = False
            date_season_end =  datetime.date(year+1, 6, 30)

            # Avoids scraping saved season data
            if os.path.isfile(status_path):
                with open(status_path, newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',')
                    line_1 = next(reader)[0]
                    if line_1 == 'complete':
                        is_complete_season = True
                        print("Player box score data found for season: " \
                              + str(year) + "-" + str(year+1))
                    elif line_1 == 'incomplete':
                        date = next(reader)
                        # Moves back 3 days to be certain saved data is complete
                        date_season_current = datetime.date(int(date[0]),\
                                                            int(date[1]),\
                                                            int(date[2])) \
                                              - datetime.timedelta(days = 3)
            else:
                # Starts at beginning of season
                date_season_current = datetime.date(year, 10, 1)
                with open(status_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(['incomplete'])
                    writer.writerow([year, 10, 1])               
                        
            if not is_complete_season:
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
                    output_file_path = "./" + self.player_file_path + season_str \
                        + "/" + str(date_season_current.year) + "_" \
                        + month_str + "_" + day_str \
                        + "_" + "player_box_scores.csv"

                    # Attempts to scrape and save data
                    pbs = client.player_box_scores(day=date_season_current.day, \
                                                   month=date_season_current.month, \
                                                   year=date_season_current.year,
                                                   output_type=OutputType.CSV,
                                                   output_file_path=output_file_path)
                    with open(output_file_path) as csv_file:
                        csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for row in csv_reader:
                            line_count += 1
                    if line_count < 2:
                        os.remove(output_file_path)
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": No games played")
                    else:
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": Game data saved")

                    # Logs data as saved
                    with open(status_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(['incomplete'])
                        writer.writerow([date_season_current.year, \
                                         date_season_current.month, \
                                         date_season_current.day])
                        
                    date_season_current = date_season_current \
                                          + datetime.timedelta(days = 1)    
                # end while
                
                # Logs scrape complete for previous seasons
                if self.date_today > date_season_current:
                    with open(status_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(['complete'])

            # end if not is_complete_season

    def scrape_data_team_box_scores(self):
        print("\nScraping team box score data.\n")
        if self.date_today.month < 10:
            current_season_start_year = self.date_today.year-1
        else:
            current_season_start_year = self.date_today.year
        year_range = range(1999, current_season_start_year+1, 1)
        for year in year_range:
            season_str = str(year) + "_" + str(year + 1)
            if not os.path.exists(self.team_file_path + season_str):
                os.mkdir(self.team_file_path + season_str)

            status_path = self.team_file_path + season_str \
                          + '/season_data_status.csv'
            is_complete_season = False
            date_season_end =  datetime.date(year+1, 6, 30)

            # Avoids scraping saved season data            
            if os.path.isfile(status_path):
                with open(status_path, newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',')
                    line_1 = next(reader)[0]
                    if line_1 == 'complete':
                        is_complete_season = True
                        print("Team box score data found for season: " \
                              + str(year) + "-" + str(year+1))
                    elif line_1 == 'incomplete':
                        date = next(reader)
                        # Moves back 3 days to be certain saved data is complete
                        date_season_current = datetime.date(int(date[0]),\
                                                            int(date[1]),\
                                                            int(date[2])) \
                                              - datetime.timedelta(days = 3)
            else:
                # Starts at beginning of season
                date_season_current = datetime.date(year, 10, 1)
                with open(status_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(['incomplete'])
                    writer.writerow([year, 10, 1])

                        
            if not is_complete_season:
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
                        
                    output_file_path = "./" + self.team_file_path + season_str \
                        + "/" + str(date_season_current.year) \
                    + "_" + month_str + "_" + day_str \
                        + "_" + "team_box_scores.csv"

                    # Attempts to scrape and save data
                    pbs = client.team_box_scores(day=date_season_current.day, \
                                                 month=date_season_current.month, \
                                                 year=date_season_current.year,\
                                                 output_type=OutputType.CSV,\
                                                 output_file_path=output_file_path)
                    with open(output_file_path) as csv_file:
                        csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for row in csv_reader:
                            line_count += 1
                    if line_count < 2:
                        os.remove(output_file_path)
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": No games played")
                    else:
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": Game data saved")

                            
                    # Logs data as saved
                    with open(status_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(['incomplete'])
                        writer.writerow([date_season_current.year, \
                                         date_season_current.month, \
                                         date_season_current.day])
                        
                    date_season_current = date_season_current \
                                          + datetime.timedelta(days = 1)    
                # end while
                
                # Logs scrape complete for previous seasons
                if self.date_today > date_season_current:
                    with open(status_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(['complete'])

                # end while
            # end if not is_complete_season
             
    def scrape_data_season_schedule(self):
        print("\nScraping season schedule data.\n")    
        if self.date_today.month < 10:
            current_season_start_year = self.date_today.year-1
        else:
            current_season_start_year = self.date_today.year
        year_range = range(2000, current_season_start_year+1, 1)
        for year in year_range:
            season_str = str(year) + "_" + str(year+1)
            output_file_path = "./" + self.season_file_path \
                + str(year) + "_" + str(year + 1) \
                + "_" + "season_schedule.csv"
            
            if os.path.isfile(output_file_path) \
               and self.date_today > datetime.date(year+1, 6, 30):
                print("Season schedule data found for season: " \
                              + str(year) + "-" + str(year+1))
            else:
                pbs = client.season_schedule(season_end_year=year+1, 
                                output_type=OutputType.CSV, 
                                output_file_path=output_file_path)
                with open(output_file_path) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    line_count = 0
                    for row in csv_reader:
                        line_count += 1
                if line_count < 2:
                    os.remove(output_file_path)
                    print("Season " + str(year) + "-" + str(year+1)\
                          + ": No games played")
                else:
                    print("Season " + str(year) + "-" + str(year+1)\
                          + ": Game data saved")
                   

    def merge_box_and_season(self):
        print("\nMerging team box scores with season data (e.g, win/loss, total pts).\n")
        if self.date_today.month < 10:
            current_season_start_year = self.date_today.year-1
        else:
            current_season_start_year = self.date_today.year
        year_range = range(2000, current_season_start_year+1, 1)
        
        for year in year_range:
            season_str = str(year) + "_" + str(year + 1)
            if not os.path.exists(self.proc_team_file_path + season_str):
                os.mkdir(self.proc_team_file_path + season_str)

            status_path = self.proc_team_file_path + season_str \
                          + '/season_data_status.csv'
            is_complete_season = False
            date_season_end =  datetime.date(year+1, 6, 30)

            # Avoids merging saved data            
            if os.path.isfile(status_path):
                with open(status_path, newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',')
                    line_1 = next(reader)[0]
                    if line_1 == 'complete':
                        is_complete_season = True
                        print("Merged team box score data found for season: " \
                              + str(year) + "-" + str(year+1))
                    elif line_1 == 'incomplete':
                        date = next(reader)
                        # Moves back 3 days to be certain saved data is complete
                        date_season_current = datetime.date(int(date[0]),\
                                                            int(date[1]),\
                                                            int(date[2])) \
                                              - datetime.timedelta(days = 3)
            else:
                # Starts at beginning of season
                date_season_current = datetime.date(year, 10, 1)
                with open(status_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(['incomplete'])
                    writer.writerow([year, 10, 1])

            
            season_file_path = season_str + "_season_schedule.csv"

            # Load season schedule
            sch_pd = pd.read_csv(self.season_file_path + season_file_path, \
                                 parse_dates=["start_time"])

            if not is_complete_season:
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
                    
                    team_temp_file_path = "./" + self.team_file_path + season_str \
                        + "/" + str(date_season_current.year) \
                        + "_" + month_str + "_" + day_str + "_" \
                        + "team_box_scores.csv"
                    processed_temp_file_path = "./" + self.proc_team_file_path \
                        + season_str + "/" + str(date_season_current.year) \
                        + "_" + month_str + "_" + day_str + "_" \
                        + "team_box_scores.csv"
                    
                    # Checks if team box score data exists for merging
                    if os.path.exists(team_temp_file_path):
                        
                        # Loads team box scores for specific date
                        team_pd = pd.read_csv(team_temp_file_path)

                        # Finds season schedule data for specific date
                        time_start_str = str(date_season_current.year) \
                                         + "-" + month_str + "-" \
                                         + day_str + ' 4:00:00'
                        time_start = pd.Timestamp(time_start_str, tz='UTC')
                        time_end = time_start + pd.Timedelta(days=1)
                        idx_start = time_start <= sch_pd["start_time"]
                        idx_end = sch_pd["start_time"] < time_end
                        sch_pd_temp = sch_pd.loc[idx_start & idx_end]

                        # Verifies games match
                        n_teams = team_pd.shape[0]
                        pts_total_1 = 0
                        for i in range(0, n_teams):
                            fg = team_pd.loc[i, "made_field_goals"]
                            three_p = team_pd.loc[i, "made_three_point_field_goals"]
                            ft = team_pd.loc[i, "made_free_throws"]
                            two_p = fg - three_p
                            pts = ft + 2*two_p + 3*three_p
                            pts_total_1 = pts_total_1 + pts
                        pts_total_2 = sch_pd_temp["away_team_score"].sum() \
                                        + sch_pd_temp["home_team_score"].sum()
                        if not pts_total_1 == pts_total_2:
                            print("FAILED TO MERGE data for date: ", time_start, \
                                  " with team box points: ", pts_total_1, \
                                  " not equal to season schedule points: ", pts_total_2)

                        # Merge data by iterating through team and schedule rows
                        n_sch_rows = sch_pd_temp.shape[0]
                        if n_sch_rows > 1:
                            for index, row in sch_pd_temp.iterrows():
                                away_team = row["away_team"]
                                away_team_score = row["away_team_score"]
                                home_team = row["home_team"]
                                home_team_score = row["home_team_score"]

                                for i in range(n_teams):
                                    if team_pd.loc[i, "team"] == home_team:
                                        team_pd.loc[i, "location"] = "home"
                                        if home_team_score > away_team_score:
                                            team_pd.loc[i, "outcome"] = "win"
                                        else:
                                            team_pd.loc[i, "outcome"] = "loss"
                                        team_pd.loc[i, "game_score"] = home_team_score
                                        team_pd.loc[i, "opponent"] = away_team
                                        team_pd.loc[i, "opponent_score"] = away_team_score
                                    elif team_pd.loc[i, "team"] == away_team:
                                        team_pd.loc[i, "location"] = "away"
                                        if home_team_score < away_team_score:
                                            team_pd.loc[i, "outcome"] = "win"
                                        else:
                                            team_pd.loc[i, "outcome"] = "loss"
                                        team_pd.loc[i, "game_score"] = away_team_score
                                        team_pd.loc[i, "opponent"] = home_team
                                        team_pd.loc[i, "opponent_score"] = home_team_score
                        else:
                            away_team = sch_pd_temp["away_team"].values[0]
                            away_team_score = sch_pd_temp["away_team_score"].values[0]
                            home_team = sch_pd_temp["home_team"].values[0]
                            home_team_score = sch_pd_temp["home_team_score"].values[0]

                            for i in range(n_teams):
                                if team_pd.loc[i, "team"] == home_team:
                                    team_pd.loc[i, "location"] = "home"
                                    if home_team_score > away_team_score:
                                        team_pd.loc[i, "outcome"] = "win"
                                    else:
                                        team_pd.loc[i, "outcome"] = "loss"
                                    team_pd.loc[i, "game_score"] = home_team_score
                                    team_pd.loc[i, "opponent"] = away_team
                                    team_pd.loc[i, "opponent_score"] = away_team_score
                                elif team_pd.loc[i, "team"] == away_team:
                                    team_pd.loc[i, "location"] = "away"
                                    if home_team_score < away_team_score:
                                        team_pd.loc[i, "outcome"] = "win"
                                    else:
                                        team_pd.loc[i, "outcome"] = "loss"
                                    team_pd.loc[i, "game_score"] = away_team_score
                                    team_pd.loc[i, "opponent"] = home_team
                                    team_pd.loc[i, "opponent_score"] = home_team_score

                        # Saves merged data to csv file
                        team_pd.to_csv(processed_temp_file_path, index=False)
                        
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": Data merged")
                        
                        # Logs data as saved
                        with open(status_path, 'w', newline='') as csvfile:
                            writer = csv.writer(csvfile, delimiter=',')
                            writer.writerow(['incomplete'])
                            writer.writerow([date_season_current.year, \
                                             date_season_current.month, \
                                             date_season_current.day])

                    else:
                        print(str(date_season_current.year) \
                              + "_" + month_str + "_" + day_str \
                              + ": No games played")
                            
                    date_season_current = date_season_current \
                                          + datetime.timedelta(days = 1) 

                # end while
                
                # Logs scrape complete for previous seasons
                if self.date_today > date_season_current:
                    with open(status_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(['complete'])
            # end if not is_complete_season
                    

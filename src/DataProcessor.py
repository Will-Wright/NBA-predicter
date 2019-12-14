# Collects and preprocesses NBA game data
#
# Requires the following web scraper utility
#    https://github.com/jaebradley/basketball_reference_web_scraper

import os
import csv
import datetime
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import OutputType   

class DataProcessor:        
    def __init__(self, year_range = range(2001, 2020, 1),\
                 root_dir = "data_raw"):
        self.year_range = year_range
        self.player_file_path = root_dir + "/player_box_scores/"
        self.team_file_path = root_dir + "/team_box_scores/"
        self.season_file_path = root_dir + "/season_schedule/"
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

    def scrape_data_player_box_scores(self):
        print("\nScraping player box score data.\n")
        for year in self.year_range:
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

                # end while
            # end if not is_complete_season

    def scrape_data_team_box_scores(self):
        print("\nScraping team box score data.\n")        
        for year in self.year_range:
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
        for year in self.year_range:
            season_str = str(year) + "_" + str(year + 1)
            output_file_path = "./" + self.season_file_path \
                + str(year) + "_" + str(year + 1) \
                + "_" + "season_schedule.csv"
            
            if os.path.isfile(output_file_path) \
               and self.date_today > datetime.date(year+1, 6, 30):
                print("Season schedule data found for season: " \
                              + str(year) + "-" + str(year+1))
            else:
                pbs = client.season_schedule(season_end_year=year, 
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
                    
                    
                    

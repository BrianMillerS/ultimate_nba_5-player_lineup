# Tools1 Final Project  
The Brians take on basketball...
  
Group Members: Brian Hanson, Brian Miller  
March 9th, 2023  

## PROJECT OVERVIEW  
The main goal of this analysis is to attempt to identify the best performing NBA 5-player lineup.  
- Lineup performance was quanified using the following metric for each entire season: (# points scored - # points allowed)/ # posessions.  
- Average lineup salary, average lineupe age, and home court advantage were the three variables significantly associated with lineup performance (glm, p<0.05)
- All player stats and play-by-play data was obtained from [www.basketball-reference.com](https://www.basketball-reference.com/).  
- For convience, play-by-play data was used from [this Kaggle dataset](https://www.kaggle.com/datasets/schmadam97/nba-playbyplay-data-20182019?resource=download)
- For the purposes of this analysis we only focused on the 2016 NBA season.  
  
## FILE DESCRIPTIONS  
  
***Bball_production.ipynb***  
The main ipython notebook for this analysis. All data aquisition (web scraping), data cleaning, summary statistics, analysis and visualization are all in this notebook. Please note that you must have all of the supporting .py files in your working dir as this notebook calls custom functions from those .py files.  
  
**play_by_play.py**  
The main python script that contains the functions necessary for: web scraping from basketball-reference.com to obtain player statistics, parsing the Kaggle play-by-play data to obtains summary statistics for each unique 5-play lineup. For more information on the intricacies involved with this process please see the "Quality of Cleaning" section of the Bball_production.ipynb notebook.  
  
**BBRscrape_boxscores.py**  
Additional support file, used by play_by_play.py.  
  
**BBRscrape_players.py**  
Additional support file, used by play_by_play.py.  
  
**hw1.ipynb**  
The original homework#1 submission, this outlines the project proposal and potential project questions.  

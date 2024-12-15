# nflsportsapi


1. Make sure to have the installed libraries:
pandas, requests, bs4(BeautifulSoup), time, os, datetime, numpy, StringIO
2. Enter the year of the season you want to extract, EX  > main(2020, True)
3. If running the current season, run the first iteration with flag = True, and for the weeks after run with flag = False

4. 13 new csv files will be created
   1. NFL_ALL_Matchups-{season}.csv >>>
      Week,Day,Date,Link,season
      
   2. NFL_Matchups-{season}.csv >>>
      Date,Link,Extracted
      
   3. NFL_Games-{season}.csv >>>
      FullTeam,PF,Result,PA,Opp,Record,Start_Time,Stadium,Coach,Date,HA,Won Toss, Won OT Toss,Roof,Surface,Duration,Attendance,Weather,Vegas Line,Over/Under,Tm,First Downs,Rush-Yds-TDs,Cmp-Att-Yd-TD-INT,Sacked-Yards,Net Pass Yards, Total Yards,Fumbles- 
      Lost,Turnovers,Penalties-Yards,Third Down Conv.,Fourth Down Conv.,Time of Possession,Link,season,Week,Day
      
   4. NFL_Passing-{season}.csv >>>
      Player,Tm,Cmp,Att,Yds,1D,1D%,IAY,IAY/PA,CAY,CAY/Cmp,CAY/PA,YAC,YAC/Cmp,Drops, Drop%,BadTh,Bad%,Sk,Bltz,Hrry,Hits,Prss,Prss%,Scrm,Yds/Scr,Pos ,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season, Pass_TDs,QB_Int, 
      QB_SackedYards,Pass_Lng,QB_Rate,Off_Fmb,Off_Fmb_Lost
      
   5. NFL_Rushing-{season}.csv >>>
      Player,Tm,Att,Yds,Rushing_TDs,1D,YBC,YBC/Att,YAC,YAC/Att,BrkTkl,Att/Br,Pos, Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season,Rushing_Lng,Off_Fmb,Off_Fmb_Lost
      
   6. NFL_Receiving-{season}.csv >>>
      Player,Tm,Tgt,Rec,Yds,Receiving_TDs,1D,YBC,YBC/R,YAC,YAC/R, ADOT,BrkTkl,Rec/Br,Drop,Drop%,Int,Rat,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season,Receiving_Lng,Off_Fmb,Off_Fmb_Lost
       
   7. NFL_Defense-{season}.csv >>>
      Player,Tm,Int,Tgt,Cmp,Cmp%,Yds,Yds/Cmp,Yds/Tgt,TD,Rat,DADOT,Air, YAC,Bltz,Hrry,QBKD,Sk,Prss,Comb,MTkl,MTkl%,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Date,Link,season,PD,TFL,QBHits,FR,FF
       
   8. NFL_Kicking-{season}.csv >>>
      Player,Tm,XPM,XPA,FGM,FGA,Pnt,Yds,Y/P,Lng,Date,Link,season
       
   9. NFL_Starters-{season}.csv >>>
      Player,Pos,Tm,Date,Link,season
       
   10. NFL_Drives-{season}.csv >>>
       Drive_Num,Quarter,Time,LOS,Plays,Length,Net Yds,Result,Tm,Date,Link,season
       
   11. NFL_Snap_Counts-{season}.csv >>>
       Player,Pos,Num,Pct,Num.1,Pct.1,Num.2,Pct.2,Starter,Tm,Date,Link,season
       
   12. NFL_Officials-{season}.csv >>>
       Pos,Name,Date,Link,season
       
   13. NFL_Pbp-{season}.csv >>>
       Quarter,Time,Down,ToGo,Location,DEN,CAR,Detail,EPB,EPA,Date,Link,season












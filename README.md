# Clash of Clans Upgrade Recommender
Clash of clans is a mobile game that I have been playing since I was in my early teens. I started playing this game the same year it was released in 2012. After playing this game for 10 years, I got older. While I still really enjoy the game, I find myself not having enough time to do the necessary research to figure out what are the best troops/buildings to prioritize upgrading.

My goal (kinda still TBD) of creating this project is to have an app that analyzes other player's game play habits to help me make a better decision on what to prioritize in this game. Hopefully, this will mean that there is less of a need for me to watch videos of content creators to figure out the important elements of the game.

# Project Status
**12/2022:** Successfully connected to Azure Table Storage, scraped data is being written and maintained there. Currently scraping gold-pass, players, and troops data, working on expanding this scope to clans, locations, and war logs.  
**11/2022:** Successfully connected to Clash of Clans API via coc.py package on a recurrent basis (Github Actions), resulting in data being scraped daily.
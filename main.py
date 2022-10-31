import argparse
import asyncio
import coc
import logging
from scraper import api
from scraper import database

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--email', help="Email account to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-p', '--password', help="Password to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-o', '--output', help="Dataset output directory.", type=str, required=False, default="./data")
    parser.add_argument('-v', '--verbosity', help="Increase output verbosity.", type=int, required=False, default=0, choices=[0,1,2])
    args = parser.parse_args()
    return args

def get_log_level(verbosity:int):
    if verbosity == 0:
        return logging.WARNING
    elif verbosity == 1:
        return logging.INFO
    else:       # verbosity == 2
        return logging.DEBUG

async def main():
    args = parse_args()

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s', 
                        level=get_log_level(args.verbosity))

    logging.info("Calling the Clash of Clans client...")
    client = coc.Client()
    await client.login(args.email, args.password)

    logging.info("Updating Gold Pass Season Table...")
    season = await client.get_current_goldpass_season()
    logging.info(season)

    await client.close()

if __name__ == '__main__':
    asyncio.run(main())

# print(Clans("%239J82VUV", token).get_current_warleague_group())         # Unknown
# print(Clans("%239J82VUV", token).get_warleague_war())                   # Unknown
# print(Clans("%239J82VUV", token).get_warlog(limit=2))                   # Success (Requires clan to set war log to public)
# print(Clans("%239J82VUV", token).search_clans(name='atas',limit=2))     # Success
# print(Clans("%239J82VUV", token).get_current_war())                     # Success (Requires clan to set war log to public)
# print(Clans("%239J82VUV", token).get_clan_info())                       # Success
# print(Clans("%239J82VUV", token).get_clan_members(limit=2))             # Success

# print(api.GoldPass(token).get_current_season())                         # Success
# database.update_goldpass_season_table(token, "./data")

# print(Players("%23Y9VG28UL", token).get_player_info())                  # Success

# print(Labels(token).get_clans_labels(limit=2))                          # Success
# print(Labels(token).get_players_labels(limit=2))                        # Success

# print(Locations("32000009", token).list_locations(limit=2))             # Success
# print(Locations("32000009", token).get_location_info())                 # Success
# print(Locations("32000009", token).get_player_versus_rankings(limit=2)) # Success (Must be location id for country)
# print(Locations("32000009", token).get_clan_versus_rankings(limit=2))   # Success (Must be location id for country)
# print(Locations("32000009", token).get_player_rankings(limit=2))        # Success (Must be location id for country)
# print(Locations("32000009", token).get_clans_rankings(limit=2))         # Success (Must be location id for country)

# print(Leagues(token).get_leagues(limit=2))                              # Success
# print(Leagues(token).get_league_season_ranking("29000022", "2015-07"))  # Success (Only for Legend League - 29000022)
# print(Leagues(token).get_league_info("29000001"))                       # Success
# print(Leagues(token).get_league_seasons("29000022", limit=2))           # Success (Only for Legend League - 29000022)
# print(Leagues(token).get_warleague_info("48000001"))                    # Success
# print(Leagues(token).get_warleagues(limit=2))                           # Success
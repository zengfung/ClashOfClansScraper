import argparse
import asyncio
import coc
import logging
from scraper import *
from scraper.gold_pass import GoldPassTableHandler

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--email', help="Email account to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-p', '--password', help="Password to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-c', '--connection_str', help="Azure Table Storage connection string.", type=str, required=True)
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
    client = coc.Client(load_game_data=coc.LoadGameData(default=True))
    await client.login(args.email, args.password)

    logging.info("Updating Gold Pass Season Table...")
    writer = GoldPassTableHandler(client, connection_string=args.connection_str)
    await writer.process_table()

    # logging.info("Updating Heroes Table...")
    # create_dataframe_for_ingame_data(client, "hero", args.output)
    
    # logging.info("Updating Pets Table...")
    # create_dataframe_for_ingame_data(client, "pet", args.output)
    
    # logging.info("Updating Troops Table...")
    # create_dataframe_for_ingame_data(client, "home_troop", args.output)
    # create_dataframe_for_ingame_data(client, "super_troop", args.output)

    # logging.info("Updating Spells Table...")
    # create_dataframe_for_ingame_data(client, "spell", args.output)

    # logging.info('Updating Players Table...')
    # await update_players_table(client, args.output)

    await client.close()

if __name__ == '__main__':
    asyncio.run(main())
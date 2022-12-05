import argparse
import asyncio
import coc
import logging
from scraper.gold_pass import GoldPassTableHandler
from scraper.in_game import TroopTableHandler
from scraper.players import PlayerTableHandler

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--email', help="Email account to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-p', '--password', help="Password to access the Clash of Clans API.", type=str, required=True)
    parser.add_argument('-n', '--name', help="Azure Table Storage account name.", type=str, required=False, default=None)
    parser.add_argument('-k', '--access_key', help="Azure Table Storage access key.", type=str, required=False, default=None)
    parser.add_argument('-c', '--connection_string', help="Azure Table Storage connection string.", type=str, required=False, default=None)
    parser.add_argument('-o', '--output', help="Dataset output directory.", type=str, required=False, default="./data")
    parser.add_argument('-v', '--verbosity', help="Increase output verbosity.", type=int, required=False, default=0, choices=[0,1,2,3,4])
    args = parser.parse_args()
    return args

def get_log_level(verbosity:int):
    match verbosity:
        case 0:
            return logging.DEBUG
        case 1:
            return logging.INFO
        case 2:
            return logging.WARNING
        case 3:
            return logging.ERROR
        case 4:
            return logging.CRITICAL
        case _:
            return logging.ERROR

async def main():
    args = parse_args()

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s', 
                        level=get_log_level(args.verbosity))

    logging.info("Calling the Clash of Clans client...")
    client = coc.Client(load_game_data=coc.LoadGameData(default=True))
    await client.login(args.email, args.password)

    logging.info("Updating Gold Pass Season Table...")
    writer = GoldPassTableHandler(client, account_name=args.name, access_key=args.access_key, connection_string=args.connection_string)
    await writer.process_table()

    logging.info("Updating Troop Table...")
    writer = TroopTableHandler(client, account_name=args.name, access_key=args.access_key, connection_string=args.connection_string)
    writer.process_table()

    logging.info("Updating Player Table...")
    writer = PlayerTableHandler(client, account_name=args.name, access_key=args.access_key, connection_string=args.connection_string)
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
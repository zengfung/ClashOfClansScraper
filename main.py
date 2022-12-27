import argparse
import asyncio
import logging
from scraper.gold_pass import GoldPassTableHandler
from scraper.troops import TroopTableHandler
from scraper.players import PlayerTableHandler
from scraper.clans import ClanTableHandler
from scraper.locations import LocationTableHandler

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

    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(module)s.%(funcName)s Line %(lineno)d : %(message)s', 
                        level=get_log_level(args.verbosity))

    login_kwargs = {
        'coc_email': args.email,
        'coc_password': args.password,
        'account_name': args.name,
        'access_key': args.access_key,
        'connection_string': args.connection_string
    }

    logging.info("Updating Gold Pass Season Table...")
    writer = GoldPassTableHandler(**login_kwargs)
    await writer.process_table()

    logging.info("Updating Troop Table...")
    writer = TroopTableHandler(**login_kwargs)
    await writer.process_table()

    logging.info("Updating Player Table...")
    writer = PlayerTableHandler(**login_kwargs)
    await writer.process_table()

    # logging.info("Updating Clan Table...")
    # writer = ClanTableHandler(client, account_name=args.name, access_key=args.access_key, connection_string=args.connection_string)
    # await writer.process_table()

    # logging.info("Updating Location Table...")
    # writer = LocationTableHandler(client, account_name=args.name, access_key=args.access_key, connection_string=args.connection_string)
    # await writer.process_table()
    # await writer.process_clan_scrape()
    # await writer.process_player_scrape()

if __name__ == '__main__':
    asyncio.run(main())
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from imports import *

from main.service import Service as Scraper_Service

def mysql_connectiontest():
    mysql_engine = create_engine(f"mysql+pymysql://{Config.MYSQL['USER']}:{Config.MYSQL['PASSWORD']}@{Config.MYSQL['HOST']}/{Config.MYSQL['DATABASE']}")
    
    try:
        start_time = time.time()
        
        with mysql_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.close()
        
        latency = (time.time() - start_time) * 1000
        
        print(f"MySQL connected! ({latency:.2f}ms)")
        return True
        
    except Exception as e:
        print(f"MySQL connection failed: {e}")
        return False
    
def initialize(module, config):
    print("\n", "=" * 60)
    print(f"Configuring {module}\n")

    #$ Scraper
    if module == 'SCRAPER':
        if Config.SCRAPER['MYSQL'] and not mysql_connectiontest():
            return
        
        schedules = Config.SCRAPER['SCHEDULER'].split(';')
        scheduler = BackgroundScheduler()

        if not schedules:
            print("No schedules configured in SCRAPER_SCHEDULER")
            return
        
        for idx, schedule in enumerate(schedules):
            try:
                hour, minute = map(int, schedule.strip().split(':'))
                scheduler.add_job(
                    Scraper_Service.initialize,
                    CronTrigger(hour=hour, minute=minute),
                    id=f'scraper_{idx}',
                    name=f'Scraper ({schedule})'
                )
                print(f"Scheduled Hours: {schedule}")
            except ValueError:
                print(f"Invalid format: {schedule} (use HH:MM)")
        
        scheduler.start()

    print("=" * 60, "\n")

if __name__ == "__main__":
    if Config.SCRAPER['ENABLED'] == 'TRUE':
        initialize("SCRAPER", Config.SCRAPER)

    while True: time.sleep(1)
from Services.DataService import BrightSpaceData as dataService

# server consts
server = 'DESKTOP-8INVJ1O\SQLEXPRESS'
username = 'DESKTOP-8INVJ1O\Max'
password = ''
driver = '{ODBC Driver 17 for SQL Server}'
trustedConnection = 'yes'
dataService = dataService(server, username, password, driver, trustedConnection)

outputDb = 'OutdoorFusion'

dataService.completeUpdateStar(outputDb)

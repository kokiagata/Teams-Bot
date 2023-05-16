import pandas as pd
import requests
import json
import pymsteams
from datetime import datetime
import pytz
import sqlalchemy
import sqlite3
from datetime import datetime
from plyer import notification
import math


def runBots():
    ticket_url = 'https://zendesk.com/api/v2/search.json?query=type:ticket group_id:${yourZdGroupId}'
    DATA_LOCATION = "sqlite:///${databaseName}.sqlite"
    engine = sqlalchemy.create_engine(DATA_LOCATION)
    connection = sqlite3.connect('${databaseName}.sqlite')
    cursor = connection.cursor()
    tableName = ${tableName}
    myTeamsMessage = pymsteams.connectorcard(${connectorUrl})
    myTeamsMessageAddition = pymsteams.connectorcard(${connectorUrl})
    myTeamsMessage.title("Ticket Updates")
    myTeamsMessageAddition.title("Additional Updates!!")
    myMessageSection = pymsteams.cardsection()
    user = ${userName}
    pwd = ${password}
    response = requests.get(ticket_url, auth=(user, pwd))
    if response.status_code != 200:
        print('Not connecting to Zendesk')
   
  # This function gets the client names
    def getTenantNames(*orgIds):
        org_frame = []
        refine_org_frame = []
        for orgId in orgIds:
            organization_url = 'https://zendesk.com/api/v2/organizations/{orgId}'.format(orgId = orgId)
            response = requests.get(organization_url, auth=(user, pwd))
            organizationList = response.json()
            org_frame.append(organizationList)
        for item in org_frame:
            refine_org_frame.append(item['organization'])
        print(refine_org_frame)
        org_dataframe = pd.json_normalize(refine_org_frame)[['id','name']]
        print(org_dataframe)
        return org_dataframe
    
    # This function lists client names
    def listTenantNames(tenantIds):
        tenantNameMapping = []
        for tenant in tenantIds:
            tenantNameMapping.append(tenant)
        tenantId = getTenantNames(*tenantNameMapping)['id']
        tenantName = getTenantNames(*tenantNameMapping)['name']
        tenantNameMapping = dict(zip(tenantId, tenantName))
        return tenantNameMapping
    
    # This function maps client id to corresponding client name
    def idToNames(ids):
        return ids.map(listTenantNames(ids))
    
    # This function gets the unix time
    def getUnixTimes(times):
        unixList = []
        for time in times:
            unixList.append(datetime.timestamp(pd.to_datetime(time)))
        return unixList
    
    # This function maps the time to corresponding unix time
    def getUnixMapping(a):
        timeList = sorted(a)
        unixes = getUnixTimes(a)
        unix = sorted(unixes)
        return dict(zip(timeList, unix))
    
    # This function gets the timezones
    def convertTimeZones(timezones):
        originalTimeZones = []
        newTimeZones = []
        for timezone in timezones:
            originalTimeZones.append(timezone)
        for timezone in originalTimeZones:
            newTimeZones.append(str(pd.to_datetime(timezone).astimezone('US/Central')))
        originalTimeZones = sorted(originalTimeZones)
        newTimeZones = sorted(newTimeZones)
        return dict(zip(originalTimeZones, newTimeZones))
    
    # This function maps the proper timezones
    def mapTimeZones(timezones):
        useThisTimezones = timezones.map(convertTimeZones(timezones))
        return useThisTimezones
    
    # Get the ticket info
    tickets = response.json()
    dataframe = []
    for ticket in tickets['results']:
        dataframe.append(ticket)
    
    # Filter to specific columns
    ticket_frame = pd.json_normalize(dataframe)[['id','group_id','status','created_at','updated_at','subject','description','priority','organization_id','url']]
    esc_ticket = ticket_frame.query('group_id==${groupId} and (status=="open" or status=="pending" or status=="hold" or status=="new")')
    if len(esc_ticket) == 0:

        queryUpdatedDate = '''
        SELECT id, updated_at FROM escalations_tickets
        '''

        cursor.execute(queryUpdatedDate)
        dbeaverObject = dict(list(cursor.fetchall()))
        dbeaverLen = len(dbeaverObject)
        if dbeaverLen > 0:

            queryToDelete = """
            Delete from escalations_tickets
            """
            esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
            cursor.execute(queryToDelete)
            print("There is no active ticket in the queue. Deleting the ticket in dbeaver")
            myTeamsMessage.text("Awesomesauce! 0 ticket in the queue!")
            myTeamsMessage.send()
            connection.close()

        else: 
            print("There is no active ticket in the queue.")
            connection.close()

    else:
        esc_ticket['organization_id'] = idToNames(esc_ticket['organization_id'])
        esc_ticket = esc_ticket.rename(columns={"organization_id":"tenant_name"})
        timeMapping = getUnixMapping(esc_ticket['updated_at'])
        esc_ticket['timestamp'] = esc_ticket['updated_at'].map(timeMapping)
        #esc_ticket['updated_at'] = tm.mapTimeZones(esc_ticket['updated_at'])
        esc_ticket['updated_at'] = mapTimeZones(esc_ticket['updated_at'])
        print(esc_ticket['updated_at'])
        queryToStore = """
        CREATE TABLE IF NOT EXISTS escalations_ticekts (
            id INT(200),
            group_id BIGINT(200),
            status VARCHAR(200),
            created_at VARCHAR(200),
            updated_at VARCHAR(200),
            subject VARCHAR(200),
            description VARCHAR(200),
            priority VARCHAR(200),
            tenant_name VARCHAR(200),
            timestamp TIMESTAMP,
            url VARCHAR(200)
            )
            """
        queryUpdatedDate = '''
        SELECT id, updated_at FROM escalations_tickets
        '''

        ## Run this once to initially set up the database
        #esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
        #cursor.execute(queryToStore)
        
        cursor.execute(queryUpdatedDate)
        dbeaverObject = dict(list(cursor.fetchall()))
        print(dbeaverObject)
        dbeaverLen = len(dbeaverObject)
        print(dbeaverLen)
        # If there are same number of tickets in zendesk and dbeaver, then we check for updated_at values for any updates
        def checkUpdatedDates(stored):
            listOfTicketId = []
            for key in stored:
                compareTickets = esc_ticket.query('id == @key')
                listOfTicketId.append(compareTickets['id'].to_string(index=False))
        # If there are same number of tickets at this point, it means no tickets got transferred or closed then got new tickets
            if len(listOfTicketId) == dbeaverLen:
                print("No different tickets")
                esc_ticket['previous_update'] = esc_ticket['id'].map(stored)
        # If updated_at values are the same, no update
                if esc_ticket['updated_at'].equals(esc_ticket['previous_update']):
                    print("No updates")
                    connection.close()

        # Otherwise, there is some update
                else:
                    print("There's some update")
                    ticketsToCheck = esc_ticket.query("updated_at != previous_update")
                    for ticketToCheck in ticketsToCheck['id']:
                        eachTicket = ticketsToCheck.query("id == @ticketToCheck")
                        print(eachTicket)
                        updatedTicketLink = eachTicket['url'].to_string(index=False)
                        refineUpdatedTicketLink = updatedTicketLink.replace("/api/v2","").replace(".json","")
                        print(refineUpdatedTicketLink)
                        updatedTicketId = eachTicket['id'].to_string(index=False)
                        print(updatedTicketId)
                        newUpdate = eachTicket['updated_at'].to_string(index=False)
                        myMessageSection.addFact("Ticket ID: ", updatedTicketId)
                        myMessageSection.addFact("Link: ", refineUpdatedTicketLink)
                        myMessageSection.addFact("Updated at: ", newUpdate)
                    myTeamsMessage.addSection(myMessageSection)
                    myTeamsMessage.text("Updates were detected")
                    myTeamsMessage.send()
                    esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                    cursor.execute(queryToStore)
                    connection.close()

        # This is when there are same number of tickets, but ids are different
            else:
                forgottenTicket = esc_ticket.query("id not in @listOfTicketId")
                forgottenTicketId = forgottenTicket['id'].to_string(index=False)
                forgottenTicketLink = forgottenTicket['url'].to_string(index=False).replace("/api/v2","").replace(".json","")
                myMessageSection.addFact("Ticket id: ", forgottenTicketId)
                myMessageSection.addFact("Link: ", forgottenTicketLink)
                myTeamsMessage.text("There are same number of tickets, but one or more tickets in the queue may be different from previous check")
                myTeamsMessage.addSection(myMessageSection)
                myTeamsMessage.send()
                esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                cursor.execute(queryToStore)
                connection.close()

        # Run this to check for ticket updates even when there are different number of tickets in the queue
        def checkPureUpdates(stored):
            listOfTicketId = []
            for key in stored:
                compareTickets = esc_ticket.query("id == @key")
                listOfTicketId.append(compareTickets['id'].to_string(index=False))
            sameTickets = esc_ticket.query("id in @listOfTicketId")
            sameTickets['previous_update'] = sameTickets['id'].map(stored)
            if sameTickets['previous_update'].equals(sameTickets['updated_at']):
                print("No updates")
                esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                cursor.execute(queryToStore)
                connection.close()

            else:
                print("There's some update")
                ticketsToCheck = sameTickets.query("updated_at != previous_update")
                for ticketToCheck in ticketsToCheck['id']:
                    eachTicket = ticketsToCheck.query("id == @ticketToCheck")
                    updatedTicketLink = eachTicket['url'].to_string(index=False)
                    refineUpdatedTicketLink = updatedTicketLink.replace("/api/v2","").replace(".json","")
                    updatedTicketId = eachTicket['id'].to_string(index=False)
                    newUpdate = eachTicket['updated_at'].to_string(index=False)
                    myMessageSection.addFact("Ticket ID: ", updatedTicketId)
                    myMessageSection.addFact("Link: ", refineUpdatedTicketLink)
                    myMessageSection.addFact("Updated at: ", newUpdate)
                myTeamsMessageAddition.addSection(myMessageSection)
                myTeamsMessageAddition.text("Additionally, some updates were detected on some tickets")
                myTeamsMessageAddition.send()
                esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                cursor.execute(queryToStore)
                connection.close()

        if len(esc_ticket) == dbeaverLen:
            print("Same number of tickets")
            checkUpdatedDates(dbeaverObject)
        
        # If there are different number of tickets, then update
        else:
            print("Different number of tickets")
            if len(esc_ticket) > dbeaverLen:
                diffLen = len(esc_ticket) - dbeaverLen
                listOfIds = []
                for ticketId in dbeaverObject:
                    listOfIds.append(ticketId)
                missingTicket = esc_ticket.query("id not in @listOfIds")
                myTeamsMessage.text("There are more tickets in the queue than last we checked!")
                myMessageSection.addFact("Difference: ", diffLen)
                for ticket in missingTicket['id']:
                    url = missingTicket.query("id == @ticket")['url'].to_string(index=False).replace("/api/v2","").replace(".json","")
                    myMessageSection.addFact("Links: ", url)
                myTeamsMessage.addSection(myMessageSection)
                myTeamsMessage.send()
        
        # check for ticket updates here
                checkPureUpdates(dbeaverObject)
                print("saved escalation tickets to dbeaver as ", tableName)
            
            if dbeaverLen > len(esc_ticket):
                diffLen = dbeaverLen - len(esc_ticket)
                listOfIdsInZd = []
                listOfIdsInDb = []
                for ticket in esc_ticket['id']:
                    listOfIdsInZd.append(ticket)
                for id in dbeaverObject:
                    listOfIdsInDb.append(id)
                goneId = list(set(listOfIdsInDb).difference(listOfIdsInZd))
                print(goneId)
                myMessageSection.addFact("Difference: ", diffLen)
                for id in goneId:
                    ticketLink = 'https://zendesk.com/api/v2/tickets/{ticket_id}'.format(ticket_id=id)
                    response = requests.get(ticketLink, auth=(user,pwd))
                    ticketUrl = response.json()['ticket']['url']
                    refineLink = ticketUrl.replace("/api/v2","").replace(".json","")
                    myMessageSection.addFact("Links: ", refineLink)
                myTeamsMessage.text("There are less tickets in the queue than last we checked!")
                myTeamsMessage.addSection(myMessageSection)
                myTeamsMessage.send()
        
        # check for ticket updates here
                checkPureUpdates(dbeaverObject)
                print("saved escalation tickets to dbeaver as ", tableName)
